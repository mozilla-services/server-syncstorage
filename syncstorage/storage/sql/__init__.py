# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
SQL backend for syncstorage.

This module implements an SQL storage plugin for synctorage.  In the simplest
use case it consists of three database tables:

  collections:  the names and ids of any custom collections
  user_collections:  the per-user metadata associated with each collection
  bso:  the individual BSO items stored in each collection

For efficiency when dealing with large datasets, the plugin also supports
sharding of the BSO items into multiple tables named "bso0" through "bsoN".
This behaviour is off by default; pass shard=True to enable it.
"""

import functools
import threading
import contextlib
from collections import defaultdict

from sqlalchemy.exc import IntegrityError

from pyramid.threadlocal import get_current_registry

from syncstorage.bso import BSO
from syncstorage.util import get_timestamp
from syncstorage.storage import (SyncStorage,
                                 ConflictError,
                                 CollectionNotFoundError,
                                 ItemNotFoundError,
                                 InvalidOffsetError)

from syncstorage.storage.sql.dbconnect import (DBConnector, MAX_TTL,
                                               BackendError)

# For efficiency, it's possible to use fixed pre-determined IDs for
# common collection names.  This is the canonical list of such names.
# Non-standard collections will be allocated IDs starting from the
# highest ID in this collection.
STANDARD_COLLECTIONS = {1: "clients", 2: "crypto", 3: "forms", 4: "history",
                        5: "keys", 6: "meta", 7: "bookmarks", 8: "prefs",
                        9: "tabs", 10: "passwords", 11: "addons"}

FIRST_CUSTOM_COLLECTION_ID = 100

MAX_COLLECTIONS_CACHE_SIZE = 1000


def ts2bigint(timestamp):
    return int(timestamp * 1000)


def bigint2ts(bigint):
    return get_timestamp(bigint / 1000.0)


def with_session(func):
    """Method decorator to magic a "session" object into existence.

    The SQLStorage backend uses a session object to manage database connections
    and related internal state.  This is not exposed in the public API, but
    is instead managed through a threadlocal storage object.

    This method decorator obtains the active session for the current thread
    (creating one if it doesn't exist) and passes it as the first argument
    to the underlying method.
    """
    @functools.wraps(func)
    def with_session_wrapper(self, *args, **kwds):
        # If the first argument is already a session object, just use that.
        if args and isinstance(args[0], SQLStorageSession):
            return func(self, *args, **kwds)
        # Otherwise, magic one into existence using threadlocals.
        with self._get_or_create_session() as session:
            return func(self, session, *args, **kwds)
    return with_session_wrapper


class SQLStorage(SyncStorage):
    """Storage plugin implemented using an SQL database.

    This class implements the storage plugin API using SQLAlchemy.  You
    must specify the SQLAlchemy database URI string to connect to, and
    can customize behaviour with the following keyword arguments:

        * standard_collections:  use fixed pre-determined ids for common
                                 collection names
        * create_tables:         create the database tables if they don't
                                 exist at startup
        * shard/shardsize:       enable sharding of the BSO table

    """

    def __init__(self, sqluri, standard_collections=False, **dbkwds):

        self.sqluri = sqluri
        self.dbconnector = DBConnector(sqluri, **dbkwds)

        # There doesn't seem to be a reliable cross-database way to set the
        # initial value of an autoincrement column.  Fake it by inserting
        # a row into the table at the desired start id.
        self.standard_collections = standard_collections
        if self.standard_collections and dbkwds.get("create_tables", False):
            zeroth_id = FIRST_CUSTOM_COLLECTION_ID - 1
            with self.dbconnector.connect() as connection:
                params = {"collectionid": zeroth_id, "name": ""}
                try:
                    connection.query("INSERT_COLLECTION", params)
                except IntegrityError:
                    pass

        # A local in-memory cache for the name => collectionid mapping.
        self._collections_by_name = {}
        self._collections_by_id = {}
        if self.standard_collections:
            for id, name in STANDARD_COLLECTIONS.iteritems():
                self._collections_by_name[name] = id
                self._collections_by_id[id] = name

        # A thread-local to track active sessions.
        self._tldata = threading.local()

    @property
    def logger(self):
        return get_current_registry()["metlog"]

    def _get_or_create_session(self):
        """Get an existing session if one exists, or start a new one if not."""
        try:
            return self._tldata.session
        except AttributeError:
            return SQLStorageSession(self)

    #
    # APIs for collection-level locking.
    #
    # For SQLite we depend on its database-level locking.  Read locks begin
    # a transaction and select something from the database, which creates
    # a SHARED lock.  Write locks begin an exlusive transaction, which
    # creates an EXCLUSIVE lock on the database.
    #
    # For MySQL (and other unsupported databases) we do explicit locking
    # on the matching row in the user_collections table.  Read locks do
    # SELECT ... LOCK IN SHARE MODE and write locks do SELECT ... FOR UPDATE.
    #
    # In theory it would be possible to use serializable transactions rather
    # than explicit locking, but our ops team have expressed concerns about
    # the efficiency of that approach at scale.
    #

    # Note: you can't use the @with_session decorator here.
    # It doesn't work right because of the generator-contextmanager thing.
    @contextlib.contextmanager
    def lock_for_read(self, userid, collection):
        """Acquire a shared read lock on the named collection."""
        with self._get_or_create_session() as session:
            try:
                collectionid = self._get_collection_id(session, collection)
            except CollectionNotFoundError:
                # If the collection doesn't exist, we still want to start
                # a transaction so it will continue to not exist.
                collectionid = 0
            # If we already have a read or write lock then
            # it's safe to use it as-is.
            if (userid, collectionid) in session.locked_collections:
                yield None
                return
            # Begin a transaction and take a lock in the database.
            params = {"userid": userid, "collectionid": collectionid}
            try:
                session.query("BEGIN_TRANSACTION_READ")
                ts = session.query_scalar("LOCK_COLLECTION_READ", params)
            except BackendError, e:
                # There is no standard exception to detect lock-wait timeouts.
                if "lock" in str(e).lower():
                    raise ConflictError
                raise
            if ts is not None:
                ts = bigint2ts(ts)
                session.cache[(userid, collectionid)].last_modified = ts
            session.locked_collections[(userid, collectionid)] = 0
            try:
                # Yield context back to the calling code.
                # This leaves the session active and holding the lock
                yield None
            finally:
                session.locked_collections.pop((userid, collectionid))

    # Note: you can't use the @with_session decorator here.
    # It doesn't work right because of the generator-contextmanager thing.
    @contextlib.contextmanager
    def lock_for_write(self, userid, collection):
        """Acquire an exclusive write lock on the named collection."""
        with self._get_or_create_session() as session:
            collectionid = self._get_collection_id(session, collection, True)
            locked = session.locked_collections.get((userid, collectionid))
            if locked == 0:
                raise RuntimeError("Can't escalate read-lock to write-lock")
            params = {"userid": userid, "collectionid": collectionid}
            try:
                session.query("BEGIN_TRANSACTION_WRITE")
                ts = session.query_scalar("LOCK_COLLECTION_WRITE", params)
            except BackendError, e:
                # There is no standard exception to detect lock-wait timeouts.
                if "lock" in str(e).lower():
                    raise ConflictError
                raise
            if ts is not None:
                ts = bigint2ts(ts)
                # Forbid the write if it would not properly incr the timestamp.
                if ts >= session.timestamp:
                    raise ConflictError
                session.cache[(userid, collectionid)].last_modified = ts
            session.locked_collections[(userid, collectionid)] = 1
            try:
                # Yield context back to the calling code.
                # This leaves the session active and holding the lock
                yield None
            finally:
                session.locked_collections.pop((userid, collectionid))

    #
    # APIs to operate on the entire storage.
    #

    @with_session
    def get_storage_timestamp(self, session, userid):
        """Returns the last-modified timestamp for the entire storage."""
        ts = session.query_scalar("STORAGE_TIMESTAMP", params={
            "userid": userid,
        }, default=0)
        return bigint2ts(ts)

    @with_session
    def get_collection_timestamps(self, session, userid):
        """Returns the collection timestamps for a user."""
        res = session.query_fetchall("COLLECTIONS_TIMESTAMPS", {
            "userid": userid,
        })
        res = self._map_collection_names(session, res)
        for collection in res:
            res[collection] = bigint2ts(res[collection])
        return res

    @with_session
    def get_collection_counts(self, session, userid):
        """Returns the collection counts."""
        res = session.query_fetchall("COLLECTIONS_COUNTS", {
            "userid": userid,
            "ttl": int(session.timestamp),
        })
        return self._map_collection_names(session, res)

    @with_session
    def get_collection_sizes(self, session, userid):
        """Returns the total size for each collection."""
        res = session.query_fetchall("COLLECTIONS_SIZES", {
            "userid": userid,
            "ttl": int(session.timestamp),
        })
        # Some db backends return a Decimal() instance for this aggregate.
        # We want just a plain old integer.
        rows = ((row[0], int(row[1])) for row in res)
        return self._map_collection_names(session, rows)

    @with_session
    def get_total_size(self, session, userid, recalculate=False):
        """Returns the total size a user's stored data."""
        size = session.query_scalar("STORAGE_SIZE", {
            "userid": userid,
            "ttl": int(session.timestamp),
        }, default=0)
        # Some db backends return a Decimal() instance for this aggregate.
        # We want just a plain old integer.
        return int(size)

    @with_session
    def delete_storage(self, session, userid):
        """Removes all data for the user."""
        session.query("DELETE_ALL_BSOS", {
            "userid": userid,
        })
        session.query("DELETE_ALL_COLLECTIONS", {
            "userid": userid,
        })

    #
    # APIs to operate on an individual collection
    #

    @with_session
    def get_collection_timestamp(self, session, userid, collection):
        """Returns the last-modified timestamp of a collection."""
        collectionid = self._get_collection_id(session, collection)
        # The last-modified timestamp may be cached on the session.
        cached_ts = session.cache[(userid, collectionid)].last_modified
        if cached_ts is not None:
            return cached_ts
        # Otherwise we need to look it up in the database.
        ts = session.query_scalar("COLLECTION_TIMESTAMP", {
            "userid": userid,
            "collectionid": collectionid,
        })
        if ts is None:
            raise CollectionNotFoundError
        return bigint2ts(ts)

    @with_session
    def get_items(self, session, userid, collection, **params):
        """Returns items from a collection."""
        return self._find_items(session, userid, collection, **params)

    @with_session
    def get_item_ids(self, session, userid, collection, **params):
        """Returns item ids from a collection."""
        params["fields"] = ["id"]
        res = self._find_items(session, userid, collection, **params)
        res["items"] = [item["id"] for item in res["items"]]
        return res

    def _find_items(self, session, userid, collection, **params):
        """Find items matching the given search parameters."""
        params["userid"] = userid
        params["collectionid"] = self._get_collection_id(session, collection)
        if "ttl" not in params:
            params["ttl"] = int(session.timestamp)
        if "newer" in params:
            params["newer"] = ts2bigint(params["newer"])
        # We always fetch one more item than necessary, so we can tell whether
        # there are additional items to be fetched with next_offset.
        limit = params.get("limit")
        if limit is not None:
            params["limit"] = limit + 1
        offset = params.get("offset")
        if offset is not None:
            try:
                params["offset"] = offset = int(offset)
            except ValueError:
                raise InvalidOffsetError(offset)
        rows = session.query_fetchall("FIND_ITEMS", params)
        items = [self._row_to_bso(row) for row in rows]
        # If the query returned no results, we don't know whether that's
        # because it's empty or because it doesn't exist.  Read the collection
        # timestamp and let it raise CollectionNotFoundError if necessary.
        if not items:
            self.get_collection_timestamp(session, userid, collection)
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
        """Convert a database table row into a BSO object."""
        item = dict(row)
        for key in ("userid", "collection", "payload_size", "ttl",):
            item.pop(key, None)
        ts = item.get("modified")
        if ts is not None:
            item["modified"] = bigint2ts(ts)
        return BSO(item)

    @with_session
    def set_items(self, session, userid, collection, items):
        """Creates or updates multiple items in a collection."""
        collectionid = self._get_collection_id(session, collection, create=1)
        rows = []
        for data in items:
            id = data["id"]
            row = self._prepare_bso_row(session, userid, collectionid,
                                        id, data)
            rows.append(row)
        defaults = {"modified": ts2bigint(session.timestamp)}
        session.insert_or_update("bso", rows, defaults)
        return self._touch_collection(session, userid, collectionid)

    @with_session
    def delete_collection(self, session, userid, collection):
        """Deletes an entire collection."""
        collectionid = self._get_collection_id(session, collection)
        count = session.query("DELETE_COLLECTION_ITEMS", {
            "userid": userid,
            "collectionid": collectionid,
        })
        count += session.query("DELETE_COLLECTION", {
            "userid": userid,
            "collectionid": collectionid,
        })
        if count == 0:
            raise CollectionNotFoundError
        return self.get_storage_timestamp(userid)

    @with_session
    def delete_items(self, session, userid, collection, items):
        """Deletes multiple items from a collection."""
        collectionid = self._get_collection_id(session, collection)
        session.query("DELETE_ITEMS", {
            "userid": userid,
            "collectionid": collectionid,
            "ids": items,
        })
        return self._touch_collection(session, userid, collectionid)

    def _touch_collection(self, session, userid, collectionid):
        """Update the last-modified timestamp of the given collection."""
        params = {
            "userid": userid,
            "collectionid": collectionid,
            "modified": ts2bigint(session.timestamp),
        }
        # The common case will be an UPDATE, so try that first.
        # If it doesn't update any rows then do an INSERT.
        rowcount = session.query("TOUCH_COLLECTION", params)
        if rowcount != 1:
            try:
                session.query("INIT_COLLECTION", params)
            except IntegrityError:
                # Someone else inserted it at the same time.
                pass
        return session.timestamp

    #
    # Items APIs
    #

    @with_session
    def get_item_timestamp(self, session, userid, collection, item):
        """Returns the last-modified timestamp for the named item."""
        collectionid = self._get_collection_id(session, collection)
        ts = session.query_scalar("ITEM_TIMESTAMP", {
            "userid": userid,
            "collectionid": collectionid,
            "item": item,
            "ttl": int(session.timestamp),
        })
        if ts is None:
            raise ItemNotFoundError
        return bigint2ts(ts)

    @with_session
    def get_item(self, session, userid, collection, item):
        """Returns one item from a collection."""
        collectionid = self._get_collection_id(session, collection)
        row = session.query_fetchone("ITEM_DETAILS", {
            "userid": userid,
            "collectionid": collectionid,
            "item": item,
            "ttl": int(session.timestamp),
        })
        if row is None:
            raise ItemNotFoundError
        return self._row_to_bso(row)

    @with_session
    def set_item(self, session, userid, collection, item, data):
        """Creates or updates a single item in a collection."""
        collectionid = self._get_collection_id(session, collection, create=1)
        row = self._prepare_bso_row(session, userid, collectionid, item, data)
        defaults = {"modified": ts2bigint(session.timestamp)}
        num_created = session.insert_or_update("bso", [row], defaults)
        return {
            "created": bool(num_created),
            "modified": self._touch_collection(session, userid, collectionid)
        }

    def _prepare_bso_row(self, session, userid, collectionid, item, data):
        """Prepare row data for storing the given BSO."""
        row = {}
        row["userid"] = userid
        row["collection"] = collectionid
        row["id"] = item
        if "sortindex" in data:
            row["sortindex"] = data["sortindex"]
        # If a payload is provided, make sure to update dependant fields.
        if "payload" in data:
            row["modified"] = ts2bigint(session.timestamp)
            row["payload"] = data["payload"]
            row["payload_size"] = len(data["payload"])
        # If provided, ttl will be an offset in seconds.
        # Add it to the current timestamp to get an absolute time.
        # If not provided or None, this means no ttl should be set.
        if "ttl" in data:
            if data["ttl"] is None:
                row["ttl"] = MAX_TTL
            else:
                row["ttl"] = data["ttl"] + int(session.timestamp)
        return row

    @with_session
    def delete_item(self, session, userid, collection, item):
        """Deletes a single item from a collection."""
        collectionid = self._get_collection_id(session, collection)
        rowcount = session.query("DELETE_ITEM", {
            "userid": userid,
            "collectionid": collectionid,
            "item": item,
            "ttl": int(session.timestamp),
        })
        if rowcount == 0:
            raise ItemNotFoundError
        return self._touch_collection(session, userid, collectionid)

    #
    # Administrative/maintenance methods.
    #

    @with_session
    def purge_expired_items(self, session, grace_period=0, max_per_loop=1000):
        """Purges items with an expired TTL from the database."""
        # Get the set of all BSO tables in the database.
        # This will be different depending on whether sharding is done.
        if not self.dbconnector.shard:
            tables = set(("bso",))
        else:
            tables = set(self.dbconnector.get_bso_table(i).name
                         for i in xrange(self.dbconnector.shardsize))
            assert len(tables) == self.dbconnector.shardsize
        # Purge each table in turn, summing rowcounts.
        # We set an upper limit on the number of iterations, to avoid
        # getting stuck indefinitely on a single table.
        total_affected = 0
        is_incomplete = False
        for table in sorted(tables):
            self.logger.info("Purging expired items from %s", table)
            num_iters = 1
            num_affected = 0
            rowcount = session.query("PURGE_SOME_EXPIRED_ITEMS", {
                "bso": table,
                "grace": grace_period,
                "maxitems": max_per_loop,
            })
            while rowcount > 0:
                num_affected += rowcount
                self.logger.debug("After %d iterations, %s items purged",
                                  num_iters, num_affected)
                num_iters += 1
                if num_iters > 100:
                    self.logger.debug("Too many iterations, bailing out.")
                    is_incomplete = True
                    break
                rowcount = session.query("PURGE_SOME_EXPIRED_ITEMS", {
                    "bso": table,
                    "grace": grace_period,
                    "maxitems": max_per_loop,
                })
            self.logger.info("Purged %d expired items from %s",
                             num_affected, table)
            total_affected += num_affected
        # Return the required data to the caller.
        # We use "is_incomplete" rather than "is_complete" in the code above
        # because we expect that, most of the time, the purge will complete.
        # So it's more efficient to flag the case when it doesn't.
        return {
            "num_purged": total_affected,
            "is_complete": not is_incomplete,
        }

    #
    # Private methods for manipulating collections.
    #

    def _get_collection_id(self, session, collection, create=False):
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
            except IntegrityError:
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
            self.logger.warn(msg % (MAX_COLLECTIONS_CACHE_SIZE,))
        else:
            self._collections_by_name[collection] = collectionid
            self._collections_by_id[collectionid] = collection


class SQLStorageSession(object):
    """Object representing a data access session.

    The SQLStorageSession object is used to encapsulate a database connection
    and transaction, along with some metadata about the snapshot of data
    begin accessed.  For example:

        * the "current time" on the server during the snapshot
        * the set of currently-locked collections

    """

    def __init__(self, storage, timestamp=None):
        self.storage = storage
        self.connection = storage.dbconnector.connect()
        self.timestamp = get_timestamp(timestamp)
        self.cache = defaultdict(SQLCachedCollectionData)
        self.locked_collections = {}
        self._nesting_level = 0

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def insert_or_update(self, table, items, defaults=None):
        """Do a bulk insert/update of the given items."""
        assert self._nesting_level > 0, "Session has not been started"
        return self.connection.insert_or_update(table, items, defaults)

    def query(self, query, params={}):
        """Execute a database query, returning the rowcount."""
        assert self._nesting_level > 0, "Session has not been started"
        return self.connection.query(query, params)

    def query_scalar(self, query, params={}, default=None):
        """Execute a database query, returning a single scalar value."""
        assert self._nesting_level > 0, "Session has not been started"
        return self.connection.query_scalar(query, params, default)

    def query_fetchone(self, query, params={}):
        """Execute a database query, returning the first result."""
        assert self._nesting_level > 0, "Session has not been started"
        return self.connection.query_fetchone(query, params)

    def query_fetchall(self, query, params={}):
        """Execute a database query, returning iterator over the results."""
        assert self._nesting_level > 0, "Session has not been started"
        return self.connection.query_fetchall(query, params)

    def begin(self):
        """Enter the context of this session.

        Calling this method marks the session as active for the current thread.
        It can be called multiple times, each requiring a corresponding call
        to either commit() or rollback().
        """
        if self._nesting_level == 0:
            assert not hasattr(self.storage._tldata, "session")
            self.storage._tldata.session = self
        self._nesting_level += 1

    def commit(self):
        """Successfully exit the context of this session.

        Once each entered context has been exited, this method will commit
        the underlying database transaction and close the connection.
        """
        self._nesting_level -= 1
        assert self._nesting_level >= 0
        if self._nesting_level == 0:
            try:
                self.connection.commit()
            finally:
                del self.storage._tldata.session
            if self.locked_collections:
                msg = "You must unlock all collections before ending a session"
                raise RuntimeError(msg)

    def rollback(self):
        """Unsuccessfully exit the context of this session.

        Once each entered context has been exited, this method will rollback
        the underlying database transaction and close the connection.
        """
        self._nesting_level -= 1
        assert self._nesting_level >= 0
        if self._nesting_level == 0:
            try:
                self.connection.rollback()
            finally:
                del self.storage._tldata.session
            if self.locked_collections:
                msg = "You must unlock all collections before ending a session"
                raise RuntimeError(msg)


class SQLCachedCollectionData(object):
    """Object for storing cached information about a collection.

    The SQLStorageSession object maintains a small cache of data that has
    already been looked up during that session.  Currently this includes only
    the last-modified timestamp of any collections locked by that session.
    """
    def __init__(self):
        self.last_modified = None
