# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Massively-sharded sqlite backend for syncstorage.

This is an experimental storage plugin for syncstorage, where each user gets
their own individual sqlite database in which to store all their stuff.  Since
we never perform any cross-user queries, there's no functional reason *not*
to keeping the data isolated like this.  So let's see if there's an operational
to actually do so...

Given a root directory in which to store data, we lay out the sqlite files
like so:

  <rootdir>/
    metadata.db      <- mostly-read-only db for shared metadata
    0/               <- 1st digit of userid, for fs sharding via symlinks
      ..
    1/
      00/            <- 2nd and 3rd digits of userid, for directory sharding
      01/
        10123456.db  <- full userid to identify the database file
        ..
      ..
    ..

TODO:

  * threading/locking/session stuff at the top level
  * capture DB errors, report as BackendError
  * the metadata db, for collection name/id mappings

"""

import os
import time
import sqlite3
import logging
import functools
import threading
import contextlib
import collections.abc

from heapdict import heapdict

from syncstorage.bso import BSO
from syncstorage.util import get_timestamp
from syncstorage.storage import (SyncStorage,
                                 ConflictError,
                                 CollectionNotFoundError,
                                 ItemNotFoundError,
                                 InvalidOffsetError)

from syncstorage.storage.sql.dbconnect import MAX_TTL, BackendError


logger = logging.getLogger("syncstorage.storage.megasqlite")

# For efficiency, we use fixed pre-determined IDs for common collection
# names.  This is the canonical list of such names.
STANDARD_COLLECTIONS = {1: "clients", 2: "crypto", 3: "forms", 4: "history",
                        5: "keys", 6: "meta", 7: "bookmarks", 8: "prefs",
                        9: "tabs", 10: "passwords", 11: "addons"}

# Non-standard collections will be allocated IDs starting from this number.
# It gives some room to grow new standard collection names in future without
# colliding with previously-stored custom collections.
FIRST_CUSTOM_COLLECTION_ID = 100

# Maximum number of collection name => id mappings to store in memory.
MAX_COLLECTIONS_CACHE_SIZE = 1000

# Maximum number of active db connections to store in memory.
MAX_DB_CACHE_SIZE = 200


def ts2bigint(timestamp):
    return int(timestamp * 1000)


def bigint2ts(bigint):
    return get_timestamp(bigint / 1000.0)


class MegaSQLiteStorage(SyncStorage):
    """Storage plugin implemented using massively-sharded sqlite."""

    def __init__(self, rootdir, **dbkwds):
        self.rootdir = rootdir

        # LRU cache of UserStorage objects, so that we're not constantly
        # opening and closing sqlite connections for sequential requests.
        self._user_storage = LRUDict(MAX_DB_CACHE_SIZE)
        self._user_storage_lock = threading.Lock()

        # A local cache for the name => collectionid mapping.
        self._collections_by_name = {}
        self._collections_by_id = {}
        for id, name in STANDARD_COLLECTIONS.iteritems():
            self._collections_by_name[name] = id
            self._collections_by_id[id] = name

        # Set the umask to ensure that any newly-created database
        # files get secure permissions by default.
        os.umask(0077)

        # XXX TODO: connect to metadata db
        # There doesn't seem to be a reliable cross-database way to set the
        # initial value of an autoincrement column.  Fake it by inserting
        # a row into the table at the desired start id.
        #self.standard_collections = standard_collections
        #if self.standard_collections and dbkwds.get("create_tables", False):
        #    zeroth_id = FIRST_CUSTOM_COLLECTION_ID - 1
        #    with self.dbconnector.connect() as connection:
        #        params = {"collectionid": zeroth_id, "name": ""}
        ##        try:
        #            connection.query("INSERT_COLLECTION", params)
        #        except sqlite3.IntegrityError:
        #            pass

    def _get_user_storage(self, userid):
        """Get the UserStorage instance for the given userid.

        This method checks an internal cache for an existing UserStorage
        instance, or creates one if it's not cached.
        """
        with self._user_storage_lock:
            try:
                ustore = self._user_storage[userid]
            except KeyError:
                ustore = UserStorage(self, userid)
                self._user_storage[userid] = ustore
            return ustore

    #
    # Private methods for manipulating collections.
    #

    def _get_collection_id(self, collection, create=False):
        """Returns a collection id, given the name.

        If the named collection does not exist then CollectionNotFoundError
        will be raised.  To automatically create collections on demand, pass
        create=True.
        """
        # Grab it from the cache if we can.
        try:
            return self._collections_by_name[collection]
        except KeyError:
            pass

        # Try to look it up in the database.
        collectionid = session.query_scalar("COLLECTION_ID", {
            "name": collection
        })
        if collectionid is None:
            # Shall we auto-create it?
            if not create:
                raise CollectionNotFoundError
            # Insert it into the database.  This might raise a conflict
            # if it was inserted concurrently by someone else.
            try:
                session.query("INSERT_COLLECTION", {
                    "collectionid": None,
                    "name": collection,
                })
            except sqlite3.IntegrityError:
                pass
            # Read the id that was created concurrently.
            collectionid = self._get_collection_id(session, collection)

        # Sanity-check that we"re not trampling standard collection ids.
        if self.standard_collections:
            assert collectionid >= FIRST_CUSTOM_COLLECTION_ID

        self._cache_collection_id(collectionid, collection)
        return collectionid

    def _get_collection_name(self, session, collectionid):
        """Returns a collection name, given the id.

        If the collection id does not exist then CollectionNotFoundError
        will be raised.
        """
        try:
            return self._collections_by_id[collectionid]
        except KeyError:
            pass

        collection = session.query_scalar("COLLECTION_NAME", {
            "collectionid": collectionid,
        })
        if collection is None:
            raise CollectionNotFoundError
        self._cache_collection_id(collectionid, collection)
        return collection

    def _load_collection_names(self, session, collection_ids):
        """Load any uncached names for the given collection ids.

        If you have a list of collection ids and you want all their names,
        use this method to prime the internal name cache.  Otherwise you"ll
        cause _get_collection_name() to do a separate database query for
        each collection id, which is very inefficient.

        Since it may not be possible to cache the names for all the requested
        collections, this method also returns a mapping from ids to names.
        """
        names = {}
        uncached_ids = []
        # Extract as many names as possible from the cache, and
        # build a list of any ids whose names are not cached.
        for id in collection_ids:
            try:
                names[id] = self._collections_by_id[id]
            except KeyError:
                uncached_ids.append(id)
        # Use a single query to fetch the names for all uncached collections.
        if uncached_ids:
            uncached_names = session.query_fetchall("COLLECTION_NAMES", {
                "ids": uncached_ids,
            })
            for id, name in uncached_names:
                names[id] = name
                self._cache_collection_id(id, name)
        # Check that we actually got a name for each specified id.
        for id in collection_ids:
            if id not in names:
                msg = "Collection id %d has no corresponding name."
                msg += "  Possible database corruption?"
                raise KeyError(msg % (id,))
        return names

    def _map_collection_names(self, session, values):
        """Helper to create a map of collection names to values.

        Given a sequence of (collectionid, value) pairs, this method will
        return a mapping from collection names to their corresponding value.
        """
        values = list(values)
        collection_ids = [collectionid for collectionid, value in values]
        names = self._load_collection_names(session, collection_ids)
        return dict([(names[id], value) for id, value in values])

    def _cache_collection_id(self, collectionid, collection):
        """Cache the given collection (id, name) pair for fast lookup."""
        if len(self._collections_by_name) > MAX_COLLECTIONS_CACHE_SIZE:
            msg = "More than %d collections have been created, "\
                  "refusing to cache them all"
            logger.warn(msg % (MAX_COLLECTIONS_CACHE_SIZE,))
        else:
            self._collections_by_name[collection] = collectionid
            self._collections_by_id[collectionid] = collection


class UserStorage(object):
    """Storage interface for a single user's data.

    This class mirrors the SyncStorage API but without the "userid"
    parameter.  It implements storage for a single user inside of an
    isolated sqlite database.
    """

    def __init__(self, owner, userid):
        self.owner = owner
        self.userid = userid

        # Shard db directory structure based on leading digits in uid.
        # 20 digits of uid should be enough for anybody...
        dbname = "%.20d.db" % (userid,)
        self.dbdir = os.path.join(owner.rootpath, dbname[0], dbname[1:3])
        self.dbpath = os.path.join(self.dbdir, dbname)

        # Connect to the db, creating it if it doesn't exist.
        try:
            self.dbcon = DBConnection(self.dbpath, self.create_tables)
        except sqlite3.OperationalError:
            # The only expected error is the db not existing yet.
            if os.path.exists(self.dbpath):
                raise
            if not os.path.exists(self.dbdir):
                os.makedirs(self.dbdir)
            # This will create an empty db file if the parent dirs exist.
            self.dbcon = DBConnection(self.dbpath, self.create_tables)
            #self._create_tables()

        # Track locking level of the current connection.
        # 1 == read lock, 2 == write lock.
        # Write locks have a fixed session timestamp.
        self.locked_level = 0
        self.locked_timestamp = None

    def get_timestamp(self):
        if self.locked_timestamp is not None:
            return self.locked_timestamp
        return get_timestamp()

    #
    # Low-level database operations.
    #

    def _create_tables(self):
        self._execute("""
            CREATE TABLE IF NOT EXISTS bsos (
                collectionid INTEGER NOT NULL,
                id TEXT NOT NULL,
                sortindex INTEGER NULL DEFAULT 0,
                modified INTEGER NOT NULL,
                payload TEXT NOT NULL DEFAULT '',
                payload_size INTEGER NOT NULL DEFAULT 0,
                ttl INTEGER NOT NULL DEFAULT %d,
                PRIMARY KEY (collectionid, id)
            )
        """ % (MAX_TTL,))
        self._execute("""
            CREATE TABLE IF NOT EXISTS collections (
                collectionid INTEGER NOT NULL PRIMARY KEY,
                last_modified INTEGER NOT NULL
            )
        """)
        self._execute("""
            CREATE INDEX IF NOT EXISTS idx_bsos_by_modified
            ON bsos (collectionid, modified)
        """)
        self._execute("""
            CREATE INDEX IF NOT EXISTS idx_bsos_by_ttl
            ON bsos (ttl)
        """)

    def execute(self, query, params=None):
        return self.dbcon.execute(query, params)

    def query(self, query, params=None):
        return self.dbcon.query(query, params)

    def query_scalar(self, query, params=None, default=None):
        return self.dbcon.query_scalar(query, params, default)

    def query_fetchone(self, query, params=None):
        return self.dbcon.query_fetchone(query, params)

    def query_fetchall(self, query, params=None):
        return self.dbcon.query_fetchall(query, params)

    #
    # APIs for collection-level locking.
    #

    @contextlib.contextmanager
    def lock_for_read(self, collection=None):
        """Acquire a shared read lock on the named collection."""
        # Locks in sqlite are for the whole db, not individual collections.
        if self.locked_level >= 1:
            yield None
        else:
            with self._do_transaction("DEFERRED"):
                self.locked_level = 1
                try:
                    yield None
                finally:
                    self.locked_level = 0

    @contextlib.contextmanager
    def lock_for_write(self, collection=None):
        """Acquire an exclusive write lock on the named collection."""
        # Locks in sqlite are for the whole db, not individual collections.
        if self.locked_level >= 2:
            yield None
        elif self.locked_level == 1:
            raise RuntimeError("Can't escalate read-lock to write-lock")
        else:
            with self._do_transaction("EXCLUSIVE"):
                self.locked_level = 2
                # Assign a fixed timestamp for all writes.
                self.locked_timestamp = get_timestamp()
                try:
                    # Check that we're not writing in the past.
                    q = "SELECT MAX(last_modified) FROM collections"
                    if self.locked_timestamp <= self.query_scalar(q):
                        raise ConflictError
                    yield None
                finally:
                    self.locked_level = 0
                    self.locked_timestamp = None

    @contextlib.contextmanager
    def _do_transaction(self, level=""):
        try:
            self.query("BEGIN %s TRANSACTION" % (level,))
        except sqlite3.Error, e:
            if "lock" in str(e).lower():
                raise ConflictError
            raise
        try:
            yield None
        except Exception:
            self.query("ROLLBACK TRANSACTION")
        else:
            self.query("COMMIT TRANSACTION")

    #
    # APIs to operate on the entire storage.
    #

    def get_storage_timestamp(self):
        q = "SELECT MAX(last_modified) FROM collections"
        ts = self.query_scalar(q, default=0)
        return bigint2ts(ts)

    def get_collection_timestamps(self):
        q = "SELECT collectionid, last_modified FROM collections"
        rows = self.query_fetchall(q)
        res = self._map_collection_names(rows)
        for collection in res:
            res[collection] = bigint2ts(res[collection])
        return res

    def get_collection_counts(self):
        q = "SELECT collectionid, COUNT(collectionid) FROM bsos "\
            "WHERE ttl>:ttl GROUP BY collectionid"
        res = self.query_fetchall(q, {
            "ttl": int(self.get_timestamp()),
        })
        return self._map_collection_names(res)

    def get_collection_sizes(self):
        q = "SELECT collectionid, SUM(payload_size) FROM bsos "\
            "WHERE ttl>:ttl GROUP BY collectionid"
        res = self.query_fetchall(q, {
            "ttl": int(self.get_timestamp()),
        })
        return self._map_collection_names(res)

    def get_total_size(self):
        q = "SELECT SUM(payload_size) FROM bsos WHERE ttl>:ttl"
        size = self.query_scalar(q, {
            "ttl": int(self.get_timestamp()),
        }, default=0)
        return size

    def delete_storage(self):
        with self.lock_for_write():
            self.query("DELETE FROM bsos")
            self.query("DELETE FROM collections")

    #
    # APIs to operate on an individual collection
    #

    def get_collection_timestamp(self, collection):
        q = "SELECT last_modified FROM collections "\
            "WHERE collectionid=:collectionid"
        collectionid = self._get_collection_id(collection)
        ts = self.query_scalar(q, {
            "collectionid": collectionid,
        })
        if ts is None:
            raise CollectionNotFoundError
        return bigint2ts(ts)

    def get_items(self, collection, **params):
        return self._find_items(collection, **params)

    def get_item_ids(self, collection, **params):
        res = self._find_items(collection, fields=["id"], **params)
        res["items"] = [item["id"] for item in res["items"]]
        return res

    def _find_items(self, collection, **params):
        params["collectionid"] = self._get_collection_id(collection)
        if "ttl" not in params:
            params["ttl"] = int(self.get_timestamp())
        # Select only the requested fields.
        fields = ",".join(params.pop("fields", ["*"]))
        q = ["SELECT", fields, "FROM bsos"]
        q.append("WHERE collectionid=:collectionid AND ttl>:ttl")
        # Apply various filters.
        if "newer" in params:
            params["newer"] = ts2bigint(params["newer"])
            q.append("AND modified > :newer")
        if "ids" in params:
            # The ids are pre-validated as base64-urlsafe so a simple
            # repr() is enough to quote them for the db here.
            q.append("AND id IN (")
            q.append(",".join(repr(id) for id in params.pop("ids")))
            q.append(")")
        # Sort in the order requested.
        # Default to "newest" so we always have a consistent ordering.
        sort = params.pop("sort", "newest")
        if sort == "index":
            q.append("ORDER BY sortindex DESC, id DESC")
        elif sort == "newest":
            q.append("ORDER BY modified DESC, id DESC")
        else:
            raise ValueError("Unexpected 'sort' value: %r" % (sort,))
        # Do limit/offset pagination if requested.
        # We always fetch one more item than necessary, so we can tell whether
        # there are additional items to be fetched with next_offset.
        limit = params.get("limit")
        if limit is not None:
            params["limit"] = limit + 1
            q.append("LIMIT :limit")
            offset = params.get("offset")
            if offset is not None:
                try:
                    params["offset"] = offset = int(offset)
                except ValueError:
                    raise InvalidOffsetError(offset)
                q.append("OFFSET :offset")
        # Slurp in all the rows and convert into BSO objects.
        rows = self.query_fetchall(" ".join(q), params)
        items = [self._row_to_bso(row) for row in rows]
        # If the query returned no results, we don't know whether that's
        # because it's empty or because it doesn't exist.  Read the collection
        # timestamp and let it raise CollectionNotFoundError if necessary.
        if not items:
            self.get_collection_timestamp(collection)
        # Check if we read past the original limit, set next_offset if so.
        next_offset = None
        if limit is not None and len(items) > limit:
            next_offset = (offset or 0) + limit
            items = items[:-1]
        return {
            "items": items,
            "next_offset": next_offset,
        }

    def _row_to_bso(self, row):
        item = dict(row)
        for key in ("collection", "payload_size", "ttl",):
            item.pop(key, None)
        ts = item.get("modified")
        if ts is not None:
            item["modified"] = bigint2ts(ts)
        return BSO(item)

    def set_items(self, collection, items):
        collectionid = self._get_collection_id(collection, create=1)
        rows = []
        for data in items:
            id = data["id"]
            row = self._prepare_bso_row(collectionid, id, data)
            rows.append(row)
        with self.lock_for_write():
            defaults = {"modified": ts2bigint(self.get_timestamp())}
            self.insert_or_update("bso", rows, defaults)
            return self._touch_collection(collectionid)

    def delete_collection(self, collection):
        collectionid = self._get_collection_id(collection)
        with self.lock_for_write():
            q = "DELETE FROM bsos WHERE collectionid=:collectionid"
            count = self.query(q, {
                "collectionid": collectionid,
            })
            q = "DELETE FROM collections WHERE collectionid=:collectionid"
            count += self.query(q, {
                "collectionid": collectionid,
            })
        if count == 0:
            raise CollectionNotFoundError
        # XXX TODO: yes, this might make the storage move backwards in time.
        # We should really track last_deleted as well as last_modified.
        return self.get_storage_timestamp()

    def delete_items(self, collection, items):
        collectionid = self._get_collection_id(collection)
        with self.lock_for_write():
            q = "DELETE FROM bsos WHERE collectionid=:collectionid "\
                "AND id in (%s)" % (",".join(repr(id) for id in items))
            self.query(q, {
                "collectionid": collectionid,
            })
            return self._touch_collection(collectionid)

    def _touch_collection(self, collectionid):
        assert self.locked_level == 2
        params = {
            "collectionid": collectionid,
            "modified": ts2bigint(self.get_timestamp()),
        }
        # The common case will be an UPDATE, so try that first.
        # If it doesn't update any rows then do an INSERT.
        qu = "UPDATE collections SET last_modified=:modified "\
             "WHERE collectionid=:collectionid"
        rowcount = self.query(qu, params)
        if rowcount != 1:
            qi = "INSERT INTO collections (collectionid, last_modified) "\
                 "VALUES (:collectionid, :modified)"
            try:
                self.query(qi, params)
            except sqlite3.IntegrityError:
                # Someone else inserted it at the same time.
                # Go again to make it the max of our and their timestamps.
                self.query(qu, params)
        return self.get_timestamp()

    #
    # Items APIs
    #

    def get_item_timestamp(self, collection, item):
        collectionid = self._get_collection_id(collection)
        q = "SELECT modified FROM bsos WHERE collectionid=:collectionid "\
            "AND id=:item AND ttl>:ttl"
        ts = self.query_scalar(q, {
            "collectionid": collectionid,
            "item": item,
            "ttl": int(self.get_timestamp()),
        })
        if ts is None:
            raise ItemNotFoundError
        return bigint2ts(ts)

    def get_item(self, collection, item):
        collectionid = self._get_collection_id(collection)
        q = "SELECT id, sortindex, modified, payload FROM bsos "\
            "WHERE collectionid=:collectionid AND id=:item AND ttl>:ttl"
        row = session.query_fetchone(q, {
            "collectionid": collectionid,
            "item": item,
            "ttl": int(self.get_timestamp()),
        })
        if row is None:
            raise ItemNotFoundError
        return self._row_to_bso(row)

    def set_item(self, collection, item, data):
        collectionid = self._get_collection_id(collection, create=True)
        row = self._prepare_bso_row(collectionid, item, data)
        with self.lock_for_write():
            defaults = {"modified": ts2bigint(self.get_timestamp())}
            num_created = self.insert_or_update("bso", [row], defaults)
            return {
                "created": bool(num_created),
                "modified": self._touch_collection(collectionid)
            }

    def _prepare_bso_row(self, collectionid, item, data):
        """Prepare row data for storing the given BSO."""
        row = {}
        row["collectionid"] = collectionid
        row["id"] = item
        if "sortindex" in data:
            row["sortindex"] = data["sortindex"]
        # If a payload is provided, make sure to update dependant fields.
        if "payload" in data:
            row["modified"] = ts2bigint(self.get_timestamp())
            row["payload"] = data["payload"]
            row["payload_size"] = len(data["payload"])
        # If provided, ttl will be an offset in seconds.
        # Add it to the current timestamp to get an absolute time.
        # If not provided or None, this means no ttl should be set.
        if "ttl" in data:
            if data["ttl"] is None:
                row["ttl"] = MAX_TTL
            else:
                row["ttl"] = data["ttl"] + int(self.get_timestamp())
        return row

    def delete_item(self, collection, item):
        collectionid = self._get_collection_id(collection)
        with self.lock_for_write():
            q = "DELETE FROM bsos WHERE collectionid=:collection "\
                "AND id=:item AND ttl>:ttl"
            rowcount = session.query(q, {
                "collectionid": collectionid,
                "item": item,
                "ttl": int(self.get_timestamp()),
            })
            if rowcount == 0:
                raise ItemNotFoundError
            return self._touch_collection(collectionid)

    #
    # Administrative/maintenance methods.
    #

    def purge_expired_items(self, grace_period=0, max_per_loop=1000):
        """Purges items with an expired TTL from the database."""
        # This method is only required to purge *some* expired items,
        # but sqlite has no equivalent of "DELETE FROM ... LIMIT X" so
        # we just delete all of them.
        q = "DELETE FROM bsos WHERE ttl < (strftime('%%s', 'now') - :grace)"
        count = self.query(q, {"grace": grace_period})
        return {
            "num_purged": count,
            "is_complete": True,
        }


class LRUDict(collections.abc.MutableMapping):
    """Simple least-recently-used cache with a dict-like interface.

    This class implements an LRU cache on top of a HeapDict.  The priority
    of each item is its last access time, which we update as items are used
    from the dict.  HeapDict operations are O(log(n)) which should be fast
    enough for our purposes, and has the advantage of constant memory use
    rather than a growing/shrinking dequeue-based solution.
    """

    def __init__(self, maxsize, curtime=None):
        if maxsize <= 0:
            raise ValueError("LRUDict maxsize must be a positive integer")
        self.maxsize = maxsize
        self.curtime = curtime or time.time
        self._items = heapdict()

    def __getitem__(self, key):
        (last_used, value) = self._items[key]
        self._items[key] = (self.curtime(), value)
        return value

    def __setitem__(self, key, value):
        if len(self._items) > self.maxsize:
            self._items.popitem()
        self._items[key] = (self.curtime(), value)

    def __delitem__(self, key):
        del self._items[key]

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


class DBConnection(object):
    """Low-level connection wrapper for sqlite."""

    def __init__(self, dbpath, create_tables_cb=None):
        self.db = sqlite3.connect(dbpath, timeout=1, isolation_level=None)
        self.dbpath = dbpath
        self.create_tables_cb = create_tables_cb

    def execute(self, query, params=None):
        """Execute a database query, returning raw result object."""
        if params is None:
            params = {}
        try:
            try:
                return self.db.execute(query, params)
            except sqlite3.OperationalError, e:
                # If some tables are missing, maybe create them on demand.
                if "no such table" in e.message:
                    if self.create_tables_cb:
                        self._create_tables_cb()
                        return self.db.execute(query, params)
                raise
        except sqlite3.OperationalError, e:
            # Wrap operational errors into a standard BackendError class.
            raise BackendError(e.message)

    def query(self, query, params=None):
        """Execute a database query, returning the rowcount."""
        res = self.execute(query, params)
        try:
            return res.rowcount
        finally:
            res.close()

    def query_scalar(self, query, params=None, default=None):
        """Execute a database query, returning a single scalar value."""
        res = self.execute(query, params)
        try:
            row = res.fetchone()
            if row is None or row[0] is None:
                return default
            return row[0]
        finally:
            res.close()

    def query_fetchone(self, query, params=None):
        """Execute a database query, returning the first result row."""
        res = self.execute(query, params)
        try:
            return res.fetchone()
        finally:
            res.close()

    def query_fetchall(self, query, params=None):
        """Execute a database query, returning list of all results."""
        res = self.execute(query, params)
        try:
            return res.fetchall()
        finally:
            res.close()
