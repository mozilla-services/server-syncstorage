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

import time
import functools
import threading
import contextlib
from collections import defaultdict

from sqlalchemy.exc import IntegrityError

from pyramid.threadlocal import get_current_registry

from syncstorage.bso import BSO
from syncstorage.util import get_new_version
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
            for id, name in STANDARD_COLLECTIONS:
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
            # Begin a transaction and take a lock in the database.
            collectionid = self._get_collection_id(session, collection)
            if (userid, collectionid) in session.locked_collections:
                raise RuntimeError("Collection already locked")
            params = {"userid": userid, "collectionid": collectionid}
            try:
                session.query("BEGIN_TRANSACTION_READ")
                ver = session.query_scalar("LOCK_COLLECTION_READ", params)
            except BackendError, e:
                # There is no standard exception to detect lock-wait timeouts.
                if "lock" in str(e).lower():
                    raise ConflictError
                raise
            if ver is None:
                raise CollectionNotFoundError
            session.cache[(userid, collectionid)].last_modified_v = ver
            session.locked_collections.add((userid, collectionid))
            try:
                # Yield context back to the calling code.
                # This leaves the session active and holding the lock
                yield None
            finally:
                session.locked_collections.remove((userid, collectionid))

    # Note: you can't use the @with_session decorator here.
    # It doesn't work right because of the generator-contextmanager thing.
    @contextlib.contextmanager
    def lock_for_write(self, userid, collection):
        """Acquire an exclusive write lock on the named collection."""
        with self._get_or_create_session() as session:
            collectionid = self._get_collection_id(session, collection, True)
            if (userid, collectionid) in session.locked_collections:
                raise RuntimeError("Collection already locked")
            params = {"userid": userid, "collectionid": collectionid}
            try:
                session.query("BEGIN_TRANSACTION_WRITE")
                ver = session.query_scalar("LOCK_COLLECTION_WRITE", params)
            except BackendError, e:
                # There is no standard exception to detect lock-wait timeouts.
                if "lock" in str(e).lower():
                    raise ConflictError
                raise
            if ver is not None:
                session.cache[(userid, collectionid)].last_modified_v = ver
            session.locked_collections.add((userid, collectionid))
            try:
                # Yield context back to the calling code.
                # This leaves the session active and holding the lock
                yield None
            finally:
                session.locked_collections.remove((userid, collectionid))

    #
    # APIs to operate on the entire storage.
    #

    @with_session
    def get_storage_version(self, session, userid):
        """Returns the last-modified version for the entire storage."""
        return session.query_scalar("STORAGE_VERSION", params={
            "userid": userid,
        }, default=0)

    @with_session
    def get_collection_versions(self, session, userid):
        """Returns the collection versions for a user."""
        res = session.query_fetchall("COLLECTIONS_VERSIONS", {
            "userid": userid,
        })
        return self._map_collection_names(session, res)

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
        return self._map_collection_names(session, res)

    @with_session
    def get_total_size(self, session, userid, recalculate=False):
        """Returns the total size a user's stored data."""
        return session.query_scalar("STORAGE_SIZE", {
            "userid": userid,
            "ttl": int(session.timestamp),
        }, default=0)

    @with_session
    def delete_storage(self, session, userid):
        """Removes all data for the user."""
        session.query("DELETE_ALL_COLLECTIONS", {
            "userid": userid,
            "version": session.new_version
        })

    #
    # APIs to operate on an individual collection
    #

    @with_session
    def get_collection_version(self, session, userid, collection):
        """Returns the last-modified version of a collection."""
        collectionid = self._get_collection_id(session, collection)
        # The last-modified version may be cached on the session.
        cached_version = session.cache[(userid, collectionid)].last_modified_v
        if cached_version is not None:
            return cached_version
        # Otherwise we need to look it up in the database.
        version = session.query_scalar("COLLECTION_VERSION", {
            "userid": userid,
            "collectionid": collectionid,
        })
        if version is None:
            raise CollectionNotFoundError
        return version

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
        # version and let it raise CollectionNotFoundError if necessary.
        if not items:
            self.get_collection_version(session, userid, collection)
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
        return BSO(item)

    @with_session
    def set_items(self, session, userid, collection, items):
        """Creates or updates multiple items in a collection."""
        collectionid = self._get_collection_id(session, collection, create=1)
        for data in items:
            id = data["id"]
            self._prepare_item_data(session, userid, collectionid, id, data)
        last_deleted = session.query_scalar("COLLECTION_LAST_DELETED", {
            "userid": userid,
            "collectionid": collectionid,
        })
        session.insert_or_update("bso", items, last_deleted)
        self._touch_collection(session, userid, collectionid)
        return session.new_version

    @with_session
    def delete_collection(self, session, userid, collection):
        """Deletes an entire collection."""
        collectionid = self._get_collection_id(session, collection)
        count = session.query("DELETE_COLLECTION", {
            "userid": userid,
            "collectionid": collectionid,
            "version": session.new_version
        })
        if count == 0:
            raise CollectionNotFoundError
        return session.new_version

    @with_session
    def delete_items(self, session, userid, collection, items):
        """Deletes multiple items from a collection."""
        collectionid = self._get_collection_id(session, collection)
        session.query("DELETE_ITEMS", {
            "userid": userid,
            "collectionid": collectionid,
            "ids": items,
        })
        self._touch_collection(session, userid, collectionid)
        return session.new_version

    def _touch_collection(self, session, userid, collectionid):
        """Update the last-modified version of the given collection."""
        params = {
            "userid": userid,
            "collectionid": collectionid,
            "version": session.new_version,
        }
        # The common case will be an UPDATE, so try that first.
        # If it doesn't update any rows then do an INSERT.
        # XXX TODO: we should refuse to move the version number backwards.
        rowcount = session.query("TOUCH_COLLECTION", params)
        if rowcount != 1:
            try:
                session.query("INIT_COLLECTION", params)
            except IntegrityError:
                # Someone else inserted it at the same time.
                pass

    #
    # Items APIs
    #

    @with_session
    def get_item_version(self, session, userid, collection, item):
        """Returns the last-modified version for the named item."""
        collectionid = self._get_collection_id(session, collection)
        version = session.query_scalar("ITEM_VERSION", {
            "userid": userid,
            "collectionid": collectionid,
            "item": item,
            "ttl": int(session.timestamp),
        })
        if version is None:
            raise ItemNotFoundError
        return version

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
        self._prepare_item_data(session, userid, collectionid, item, data)
        print "PREPARED ITEM", data
        last_deleted = session.query_scalar("COLLECTION_LAST_DELETED", {
            "userid": userid,
            "collectionid": collectionid,
        })
        num_created = session.insert_or_update("bso", [data], last_deleted)
        self._touch_collection(session, userid, collectionid)
        return {
            "created": bool(num_created),
            "version": session.new_version,
        }

    def _prepare_item_data(self, session, userid, collectionid, item, data):
        """Fill in and normalize fields in the given item data."""
        data["userid"] = userid
        data["collection"] = collectionid
        data["id"] = item
        # If a payload is provided, make sure to update dependant fields.
        if "payload" in data:
            # XXX TODO: we need to ensure version and timestamp get set
            # on creation, even if we default to a payload of null
            data["version"] = session.new_version
            data["timestamp"] = int(session.timestamp * 1000)
            data["payload_size"] = len(data["payload"])
        # If provided, ttl will be an offset in seconds.
        # Add it to the current timestamp to get an absolute time.
        # If not provided or None, this means no ttl should be set.
        if "ttl" in data:
            if data["ttl"] is None:
                data["ttl"] = MAX_TTL
            else:
                data["ttl"] += int(session.timestamp)
        return data

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
        self._touch_collection(session, userid, collection)
        return session.new_version

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

        Since it may not be possible to cache the names for all the request
        collections, this method also returns a mapping from ids to names.
        """
        names = {}
        uncached_ids = []
        # Extract as many names as possible from the cache, and
        # build a list of any ids whose names are not cached.
        for id in collection_ids:
            try:
                names[id] = self._collections_by_name[id]
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

    @with_session
    def dump_items(self, session):
        print "===ITEMS=="
        for row in session.query_fetchall("ALL_ITEMS"):
            print row
        print "=========="

    @with_session
    def dump_collections(self, session):
        print "===COLLECTIONS==="
        for row in session.query_fetchall("ALL_COLLECTIONS"):
            print row
        print "=========="


class SQLStorageSession(object):
    """Object representing a data access session.

    The SQLStorageSession object is used to encapsulate a database connection
    and transaction, along with some metadata about the snapshot of data
    begin accessed.  For example:

        * the "current time" on the server during the snapshot
        * the new version number to be associated with any writes
        * the last-modified versions of collections
        * the set of currently-locked collections
    """

    def __init__(self, storage):
        self.storage = storage
        self.connection = storage.dbconnector.connect()
        self.new_version = get_new_version()
        self.timestamp = time.time()
        self.cache = defaultdict(SQLCachedCollectionData)
        self.locked_collections = set()
        self._nesting_level = 0

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def insert_or_update(self, table, items, last_deleted=None):
        """Do a bulk insert/update of the given items."""
        assert self._nesting_level > 0, "Session has not been started"
        return self.connection.insert_or_update(table, items, last_deleted)

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
    the last-modified version of any collections locked by that session.
    """
    def __init__(self):
        self.last_modified_v = None
