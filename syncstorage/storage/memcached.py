# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Memcached backend wrapper for syncstorage.

This module implements a memcached layer for the SyncStorage backend API.
It caches frequently-used metadata in memcache while passing the bulk of
the operations on to an underlying backend implementation.  It is also capable
of storing entire collections in memcache without hitting the backend.

The following memcached keys are used:

    * userid:metadata         metadata about the storage and collections
    * userid:c:<collection>   cached data for a particular collection

A key prefix can also be defined to avoid clobbering unrelated data in a
shared memcached setup.  It defaults to the empty string.

The "metadata" key contains a JSON object describing the state of the store.
The data is all stored as a single key so that it can be updated atomically.
It has the following structure:

    {
      "size":               <approximate total size of the stored data>,
      "last_size_recalc":   <time when size was last recalculated>,
      "modified":           <last-modified timestamp for the entire storage>,
      "collections": {
         <collection name>:  <last-modified timestamp for the collection>,
      },
    }

For each collection to be stored in memcache, the corresponding key contains
a JSON mapping from item ids to BSO objects along with a record of the last-
modified timestamp for that collection:

    {
      "modified":   <last-modified timestamp for the collection>,
      "items": {
        <item id>:  <BSO object for that item>,
      }
    }

To avoid the cached data getting out of sync with the underlying storage, we
explicitly mark the cache as dirty before performing any write operations.
In the unlikely event of a mid-operation crash, we'll notice the dirty cache
and fall back to the underlying store instead of using potentially inconsistent
data from memcache.
"""

import time
import threading
import contextlib

from syncstorage.util import get_timestamp, json_loads, json_dumps
from syncstorage.storage import (SyncStorage,
                                 StorageError,
                                 ConflictError,
                                 CollectionNotFoundError,
                                 ItemNotFoundError,
                                 InvalidOffsetError,
                                 InvalidBatch)

from pyramid.settings import aslist

from mozsvc.storage.mcclient import MemcachedClient


# Recalculate quota at most once per hour.
SIZE_RECALCULATION_PERIOD = 60 * 60

# Expire cache-based lock after five minutes.
DEFAULT_CACHE_LOCK_TTL = 5 * 60

# Grace period to allow between expiring of ttl's items, and deletion.
TTL_EXPIRY_GRACE_PERIOD = 60 * 60 * 24  # 1 day, in seconds


def _key(*names):
    return ":".join(map(str, names))


def bso_sort_key_index(bso):
    return (bso["sortindex"], bso["id"])


def bso_sort_key_modified(bso):
    return (bso["modified"], bso["id"])


class MemcachedClient(MemcachedClient):
    """MemcachedClient that can handle decimal.Decimal instances."""

    def _encode_value(self, value):
        value = json_dumps(value)
        if len(value) > self.max_value_size:
            raise ValueError("value too long")
        return value, 0

    def _decode_value(self, value, flags):
        return json_loads(value)


class MemcachedStorage(SyncStorage):
    """Memcached caching wrapper for SyncStorage backends.

    The SyncStorage implementation wraps another storage backend to provide
    a caching layer.  You may specify the following arguments:

        * storage:  the underlying SyncStorage object that is to be wrapped.
        * cache_servers:  a list of memcached server URLs.
        * cached_collections:  a list of names of collections that should
                               be duplicated into memcache for fast access.
        * cache_only_collections:  a list of names of collections that should
                                   be stored *only* in memcached, and never
                                   written through to the bacend.
        * cache_key_prefix:  a string to be prepended to all memcached keys,
                             useful for namespacing in shared cache setups.
        * cache_pool_size:  the maximum number of active memcache clients.
        * cache_pool_timeout:  the maximum lifetime of each memcache client.

    """

    def __init__(self, storage, cache_servers=None, cache_key_prefix="",
                 cache_pool_size=None, cache_pool_timeout=60,
                 cached_collections=(), cache_only_collections=(),
                 cache_lock=False, cache_lock_ttl=None, **kwds):
        self.storage = storage
        self.cache = MemcachedClient(cache_servers, cache_key_prefix,
                                     cache_pool_size, cache_pool_timeout)
        self.cached_collections = {}
        for collection in aslist(cached_collections):
            colmgr = CachedManager(self, collection)
            self.cached_collections[collection] = colmgr
        self.cache_only_collections = {}
        for collection in aslist(cache_only_collections):
            colmgr = CacheOnlyManager(self, collection)
            self.cache_only_collections[collection] = colmgr
        self.cache_lock = cache_lock
        if cache_lock_ttl is None:
            self.cache_lock_ttl = DEFAULT_CACHE_LOCK_TTL
        else:
            self.cache_lock_ttl = cache_lock_ttl
        # Keep a threadlocal to track the currently-held locks.
        # This is needed to make the read locking API reentrant.
        self._tldata = threading.local()

    def iter_cache_keys(self, userid):
        """Iterator over all potential cache keys for the given userid.

        This method yields all potential cache keys for the given userid,
        including their metadata key and the keys for any cached collections.
        The yielded keys do *not* include the key prefix, if any.
        """
        yield _key(userid, "metadata")
        for colmgr in self.cached_collections.itervalues():
            yield colmgr.get_key(userid)
        for colmgr in self.cache_only_collections.itervalues():
            yield colmgr.get_key(userid)

    def _get_collection_manager(self, collection):
        """Get a collection-management object for the named collection.

        This class delegates all collection-level operations to a "collection
        manager" object.  The manager for a given collection will be different
        depending on the required caching characteristics, and this method
        gets and returns on appropriate manager for the named collection.
        """
        try:
            return self.cached_collections[collection]
        except KeyError:
            try:
                return self.cache_only_collections[collection]
            except KeyError:
                return UncachedManager(self, collection)

    #
    # APIs for collection-level locking.
    #
    # This class provides the option of locking at the memcache level rather
    # than calling through to the underlying storage engine.  Such locks
    # are just simple mutex keys in memcache, one per collection.  If you
    # can successfully add the key then you get the lock, if it already
    # exists then someone else holds the lock.  If you crash while holding
    # the lock, it will eventually expire.
    #

    def lock_for_read(self, userid, collection):
        """Acquire a shared read lock on the named collection."""
        if self.cache_lock or collection in self.cache_only_collections:
            return self._lock_in_memcache(userid, collection)
        else:
            return self.storage.lock_for_read(userid, collection)

    def lock_for_write(self, userid, collection):
        """Acquire an exclusive write lock on the named collection."""
        if self.cache_lock or collection in self.cache_only_collections:
            return self._lock_in_memcache(userid, collection)
        else:
            return self.storage.lock_for_write(userid, collection)

    @contextlib.contextmanager
    def _lock_in_memcache(self, userid, collection):
        """Helper method to take a memcache-level lock on a collection."""
        # Use a thread-local set of held locks to make this reentrant.
        try:
            locked_collections = self._tldata.locked_collections
        except AttributeError:
            locked_collections = self._tldata.locked_collections = set()
        if (userid, collection) in locked_collections:
            yield None
            return
        # Take the lock in memcached.
        ttl = self.cache_lock_ttl
        now = time.time()
        key = _key(userid, "lock", collection)
        if not self.cache.add(key, True, time=ttl):
            raise ConflictError
        locked_collections.add((userid, collection))
        try:
            yield None
        finally:
            locked_collections.remove((userid, collection))
            if time.time() - now >= ttl:
                msg = "Lock expired while we were holding it"
                raise RuntimeError(msg)
            self.cache.delete(key)

    #
    # APIs to operate on the entire storage.
    #

    def get_storage_timestamp(self, userid):
        """Returns the last-modified timestamp for the entire storage."""
        # Try to use the cached value.
        ts = self._get_metadata(userid)["modified"]
        # Fall back to live data if it's dirty.
        if ts is None:
            ts = self.storage.get_storage_timestamp(userid)
            for colmgr in self.cache_only_collections.itervalues():
                try:
                    ts = max(ts, colmgr.get_timestamp(userid))
                except CollectionNotFoundError:
                    pass
        return ts

    def get_collection_timestamps(self, userid):
        """Returns the collection timestamps for a user."""
        # Try to use the cached value.
        timestamps = self._get_metadata(userid)["collections"]
        # Fall back to live data for any collections that are dirty.
        for collection, ts in timestamps.items():
            if ts is None:
                colmgr = self._get_collection_manager(collection)
                try:
                    timestamps[collection] = colmgr.get_timestamp(userid)
                except CollectionNotFoundError:
                    del timestamps[collection]
        return timestamps

    def get_collection_counts(self, userid):
        """Returns the collection counts."""
        # Read most of the data from the database.
        counts = self.storage.get_collection_counts(userid)
        # Add in counts for collections stored only in memcache.
        for colmgr in self.cache_only_collections.itervalues():
            try:
                items = colmgr.get_items(userid)["items"]
            except CollectionNotFoundError:
                pass
            else:
                counts[colmgr.collection] = len(items)
        return counts

    def get_collection_sizes(self, userid):
        """Returns the total size for each collection."""
        # Read most of the data from the database.
        sizes = self.storage.get_collection_sizes(userid)
        # Add in sizes for collections stored only in memcache.
        for colmgr in self.cache_only_collections.itervalues():
            try:
                items = colmgr.get_items(userid)["items"]
                payloads = (item.get("payload", "") for item in items)
                sizes[colmgr.collection] = sum(len(p) for p in payloads)
            except CollectionNotFoundError:
                pass
        # Since we've just gone to the trouble of recalculating sizes,
        # we might as well update the cached total size as well.
        self._update_total_size(userid, sum(sizes.itervalues()))
        return sizes

    def get_total_size(self, userid, recalculate=False):
        """Returns the total size of a user's storage data."""
        return self._get_metadata(userid, recalculate)["size"]

    def delete_storage(self, userid):
        """Removes all data for the user."""
        for key in self.iter_cache_keys(userid):
            self.cache.delete(key)
        self.storage.delete_storage(userid)

    #
    # APIs to operate on an individual collection
    #

    def get_collection_timestamp(self, userid, collection):
        """Returns the last-modified timestamp for the named collection."""
        # It's likely cheaper to read all cached timestamps out of memcache
        # than to read just the single timestamp from the database.
        timestamps = self.get_collection_timestamps(userid)
        try:
            ts = timestamps[collection]
        except KeyError:
            raise CollectionNotFoundError
        # Refresh from the live data if dirty.
        if ts is None:
            colmgr = self._get_collection_manager(collection)
            ts = colmgr.get_timestamp(userid)
        return ts

    def get_items(self, userid, collection, **kwds):
        """Returns items from a collection"""
        colmgr = self._get_collection_manager(collection)
        return colmgr.get_items(userid, **kwds)

    def get_item_ids(self, userid, collection, **kwds):
        """Returns item idss from a collection"""
        colmgr = self._get_collection_manager(collection)
        return colmgr.get_item_ids(userid, **kwds)

    def set_items(self, userid, collection, items):
        """Creates or updates multiple items in a collection."""
        colmgr = self._get_collection_manager(collection)
        with self._mark_collection_dirty(userid, collection) as update:
            ts = colmgr.set_items(userid, items)
            size = sum(len(item.get("payload", "")) for item in items)
            update(ts, ts, size)
            return ts

    def delete_collection(self, userid, collection):
        """Deletes an entire collection."""
        colmgr = self._get_collection_manager(collection)
        with self._mark_collection_dirty(userid, collection) as update:
            ts = colmgr.del_collection(userid)
            update(ts, None)
            return ts

    def delete_items(self, userid, collection, items):
        """Deletes multiple items from a collection."""
        colmgr = self._get_collection_manager(collection)
        with self._mark_collection_dirty(userid, collection) as update:
            ts = colmgr.del_items(userid, items)
            update(ts, ts)
            return ts

    def create_batch(self, userid, collection):
        """Creates batch for a give user's collection."""
        colmgr = self._get_collection_manager(collection)
        return colmgr.create_batch(userid)

    def valid_batch(self, userid, collection, batchid):
        """Verifies that a batch ID is valid"""
        colmgr = self._get_collection_manager(collection)
        return colmgr.valid_batch(userid, batchid)

    def append_items_to_batch(self, userid, collection, batchid, items):
        """Appends items to the pending batch."""
        colmgr = self._get_collection_manager(collection)
        ts = colmgr.append_items_to_batch(userid, batchid, items)
        return ts

    def apply_batch(self, userid, collection, batchid):
        """Applies the batch"""
        colmgr = self._get_collection_manager(collection)
        # prevts = colmgr.get_timestamp(userid)
        with self._mark_collection_dirty(userid, collection):
            ts = colmgr.apply_batch(userid, batchid)
            # Lazy update should occur on following get_item/s
        return ts

    def close_batch(self, userid, collection, batchid):
        colmgr = self._get_collection_manager(collection)
        return colmgr.close_batch(userid, batchid)

    #
    # Items APIs
    #

    def get_item_timestamp(self, userid, collection, item):
        """Returns the last-modified timestamp for the named item."""
        colmgr = self._get_collection_manager(collection)
        return colmgr.get_item_timestamp(userid, item)

    def get_item(self, userid, collection, item):
        """Returns one item from a collection."""
        colmgr = self._get_collection_manager(collection)
        return colmgr.get_item(userid, item)

    def set_item(self, userid, collection, item, data):
        """Creates or updates a single item in a collection."""
        colmgr = self._get_collection_manager(collection)
        with self._mark_collection_dirty(userid, collection) as update:
            res = colmgr.set_item(userid, item, data)
            size = len(data.get("payload", ""))
            update(res["modified"], res["modified"], size)
            return res

    def delete_item(self, userid, collection, item):
        """Deletes a single item from a collection."""
        colmgr = self._get_collection_manager(collection)
        with self._mark_collection_dirty(userid, collection) as update:
            ts = colmgr.del_item(userid, item)
            update(ts, ts)
            return ts

    #
    # Administrative/maintenance methods.
    #

    def purge_expired_items(self, grace_period=0, max_per_loop=1000):
        """Purges items with an expired TTL from the database."""
        # We have no way to purge expired items from memcached, as
        # there's no way to enumerate all the userids.  Purging is
        # instead done on each write for cached collections, with the
        # expectation that this will be cheap due to low item count.
        # Therefore, the only thing we can do here is pass on the call.
        return self.storage.purge_expired_items(grace_period, max_per_loop)

    #
    #  Private APIs for managing the cached metadata
    #

    def _get_metadata(self, userid, recalculate_size=False):
        """Get the metadata dict, recalculating things if necessary.

        This method pulls the dict of metadata out of memcache and returns it.
        If there is no information yet in memcache then it pulls the data from
        the underlying storage, caches it and then returns it.

        If recalculate_size is given and True, then the cache size value will
        be recalculated from the store if it is more than an hour old.
        """
        key = _key(userid, "metadata")
        data, casid = self.cache.gets(key)
        # If there is no cached metadata, initialize it from the storage.
        # Use CAS to avoid overwriting other changes, but don't error out if
        # the write fails - it just means that someone else beat us to it.
        if data is None:
            # Get the mapping of collection names to timestamps.
            # Make sure to include any cache-only collections.
            timestamps = self.storage.get_collection_timestamps(userid)
            for colmgr in self.cached_collections.itervalues():
                if colmgr.collection not in timestamps:
                    try:
                        ts = colmgr.get_timestamp(userid)
                        timestamps[colmgr.collection] = ts
                    except CollectionNotFoundError:
                        pass
            # Get the storage-level modified time.
            # Make sure it's not less than any collection-level timestamp.
            ts = self.storage.get_storage_timestamp(userid)
            if timestamps:
                ts = max(ts, max(timestamps.itervalues()))
            # Calculate the total size if requested,
            # but don't bother if it's not necessary.
            if not recalculate_size:
                last_size_recalc = 0
                size = 0
            else:
                last_size_recalc = int(time.time())
                size = self._recalculate_total_size(userid)
            # Store it all back into the cache.
            data = {
                "size": size,
                "last_size_recalc": last_size_recalc,
                "modified": ts,
                "collections": timestamps,
            }
            self.cache.cas(key, data, casid)
        # Recalculate the size if it appears to be out of date.
        # Use CAS to avoid clobbering changes but don't let it fail us.
        elif recalculate_size:
            recalc_period = time.time() - data["last_size_recalc"]
            if recalc_period > SIZE_RECALCULATION_PERIOD:
                data["last_size_recalc"] = int(time.time())
                data["size"] = self._recalculate_total_size(userid)
                self.cache.cas(key, data, casid)
        return data

    def _update_total_size(self, userid, size):
        """Update the cached value for total storage size."""
        key = _key(userid, "metadata")
        data, casid = self.cache.gets(key)
        if data is None:
            self._get_metadata(userid)
            data, casid = self.cache.gets(key)
        data["last_size_recalc"] = int(time.time())
        data["size"] = size
        self.cache.cas(key, data, casid)

    def _recalculate_total_size(self, userid):
        """Re-calculate total size from the database."""
        size = self.storage.get_total_size(userid)
        for colmgr in self.cache_only_collections.itervalues():
            try:
                items = colmgr.get_items(userid)["items"]
                payloads = (item.get("payload", "") for item in items)
                size += sum(len(p) for p in payloads)
            except CollectionNotFoundError:
                pass
        return size

    @contextlib.contextmanager
    def _mark_collection_dirty(self, userid, collection):
        """Context manager for marking collections as dirty during write.

        To prevent the cache from getting out of sync with the underlying store
        it is necessary to mark a collection as dirty before performing any
        modifications on it.  This is a handy context manager that can take
        care of that, as well as update the timestamps with new results when
        the modification is complete.

        The context object associated with this method is a callback function
        that can be used to update the stored metadata.  It accepts the top-
        level storage timestamp, collection-level timestamp, and a total size
        increment as its three arguments.  Example usage::

            with self._mark_collection_dirty(userid, collection) as update:
                colobj = self._get_collection_manager(collection)
                ts = colobj.set_item(userid, "test", {"payload": "TEST"})
                update(ts, ts, len("TEST"))

        """
        # Get the old values from the metadata.
        # We can't call _get_metadata directly because we also want the casid.
        key = _key(userid, "metadata")
        data, casid = self.cache.gets(key)
        if data is None:
            # No cached data, so refresh.
            self._get_metadata(userid)
            data, casid = self.cache.gets(key)

        # Write None into the metadata to mark things as dirty.
        ts = data["modified"]
        col_ts = data["collections"].get(collection)
        data["modified"] = None
        data["collections"][collection] = None
        if not self.cache.cas(key, data, casid):
            raise ConflictError

        # Define the callback function for the calling code to use.
        # We also use this function internally to recover from errors.
        update_was_called = []

        def update(ts=ts, col_ts=col_ts, size_incr=0):
            assert not update_was_called
            update_was_called.append(True)
            data["modified"] = ts
            if col_ts is None:
                del data["collections"][collection]
            else:
                data["collections"][collection] = col_ts
            data["size"] += size_incr
            # We assume the write lock is held to avoid conflicting changes.
            # Sadly, using CAS again would require another round-trip.
            self.cache.set(key, data)

        # Yield out to the calling code.
        # It can call the yielded function to provide new metadata.
        # If they don't call it, then we cannot make any assumptions about
        # the consistency of the cached data and must leave things marked
        # as dirty until another write cleans it up.
        try:
            yield update
        except StorageError:
            # If a storage-related error occurs, then we know that the
            # operation wrapped by the calling code did not succeed.
            # It's therefore safe to roll back to previous values.
            if not update_was_called:
                update()
            raise


#  Collections stored in the MemcachedStorage class can have different
#  behaviours associated with them, depending on whether they are not
#  cached at all, cached in write-through mode, or cached without writing
#  back to the underlying store.  To simplify the code, we break out the
#  operations for each type of collection into a "manager" class.

class UncachedManager(object):
    """Manager class for collections that are not stored in memcache at all.

    This class provides methods for operating on a collection that is stored
    only in the backing store, not in memcache.  It just passes the method
    calls through, and exists only to simplify the main API by providing a
    common interface to all types of collection.
    """

    def __init__(self, owner, collection):
        self.owner = owner
        self.collection = collection

    def get_timestamp(self, userid):
        storage = self.owner.storage
        return storage.get_collection_timestamp(userid, self.collection)

    def get_items(self, userid, **kwds):
        storage = self.owner.storage
        return storage.get_items(userid, self.collection, **kwds)

    def get_item_ids(self, userid, **kwds):
        storage = self.owner.storage
        return storage.get_item_ids(userid, self.collection, **kwds)

    def set_items(self, userid, items):
        storage = self.owner.storage
        return storage.set_items(userid, self.collection, items)

    def del_collection(self, userid):
        storage = self.owner.storage
        return storage.delete_collection(userid, self.collection)

    def del_items(self, userid, items):
        storage = self.owner.storage
        return storage.delete_items(userid, self.collection, items)

    def get_item_timestamp(self, userid, item):
        storage = self.owner.storage
        return storage.get_item_timestamp(userid, self.collection, item)

    def get_item(self, userid, item):
        storage = self.owner.storage
        return storage.get_item(userid, self.collection, item)

    def set_item(self, userid, item, bso):
        storage = self.owner.storage
        return storage.set_item(userid, self.collection, item, bso)

    def del_item(self, userid, item):
        storage = self.owner.storage
        return storage.delete_item(userid, self.collection, item)

    def create_batch(self, userid):
        storage = self.owner.storage
        return storage.create_batch(userid, self.collection)

    def valid_batch(self, userid, batchid):
        storage = self.owner.storage
        return storage.valid_batch(userid, self.collection, batchid)

    def append_items_to_batch(self, userid, batchid, items):
        storage = self.owner.storage
        return storage.append_items_to_batch(userid, self.collection, batchid,
                                             items)

    def apply_batch(self, userid, batchid):
        storage = self.owner.storage
        return storage.apply_batch(userid, self.collection, batchid)

    def close_batch(self, userid, batchid):
        storage = self.owner.storage
        return storage.close_batch(userid, self.collection, batchid)


class _CachedManagerBase(object):
    """Common functionality for CachedManager and CacheOnlyManager.

    This class holds the duplicated logic between our two different types
    of in-cache collection managers: collections that are both in the cacha
    and in the backing store, and collections that exist solely in memcache.
    """

    def __init__(self, owner, collection):
        self.owner = owner
        self.collection = collection

    def get_key(self, userid):
        return _key(userid, "c", self.collection)

    @property
    def storage(self):
        return self.owner.storage

    @property
    def cache(self):
        return self.owner.cache

    #
    # Methods that need to be implemented by subclasses.
    # All the rest of the functionality is implemented in terms of these.
    #

    def get_cached_data(self, userid):
        raise NotImplementedError

    def set_items(self, userid, items):
        raise NotImplementedError

    def del_collection(self, userid):
        raise NotImplementedError

    def del_items(self, userid, items):
        raise NotImplementedError

    def set_item(self, userid, item, bso):
        raise NotImplementedError

    def del_item(self, userid, item):
        raise NotImplementedError

    #
    # Helper methods for updating cached collection data.
    # Subclasses use this common logic for updating the cache, but
    # need to layer different steps around it.
    #

    def _set_items(self, userid, items, modified, data, casid):
        """Update the cached data by setting the given items.

        This method performs the equivalent of SyncStorage.set_items() on
        the cached data.  You must provide the new last-modified timestamp,
        the existing data dict, and the casid of the data currently stored
        in memcache.

        It returns the number of items that were newly created, which may
        be less than the number of items given if some already existed in
        the cached data.
        """
        if not data:
            data = {"modified": modified, "items": {}}
        elif data["modified"] >= modified:
            raise ConflictError
        num_created = 0
        for item in items:
            # Cache only the fields we need.
            bso = {}
            bso["id"] = item["id"]
            if "payload" in item:
                bso["payload"] = item["payload"]
                bso["modified"] = modified
            if "sortindex" in item:
                bso["sortindex"] = item["sortindex"]
            if "ttl" in item:
                # ttl is given as an offset; make it an absolute time.
                if item["ttl"] is None:
                    bso["ttl"] = None
                else:
                    bso["ttl"] = int(modified) + item["ttl"]
            # Update it in-place, or create if it doesn't exist.
            try:
                data["items"][bso["id"]].update(bso)
            except KeyError:
                num_created += 1
                # Set default payload on newly-created items.
                bso["modified"] = modified
                if "payload" not in bso:
                    bso["payload"] = ""
                data["items"][bso["id"]] = bso
                data["modified"] = modified
        # Purge any items that have expired.
        # We can't do this as part of the purge_expired_items()
        # because we don't have a way to enumerate all user ids.
        expired_ids = set()
        expiry_time = int(time.time()) - TTL_EXPIRY_GRACE_PERIOD
        for id, bso in data["items"].iteritems():
            ttl = bso.get("ttl")
            if ttl is not None and ttl < expiry_time:
                expired_ids.add(id)
        for id in expired_ids:
            del data["items"][id]
        key = self.get_key(userid)
        if not self.cache.cas(key, data, casid):
            raise ConflictError
        return num_created

    def _del_items(self, userid, items, modified, data, casid):
        """Update the cached data by deleting the given items.

        This method performs the equivalent of SyncStorage.delete_items() on
        the cached data.  You must provide the new last-modified timestamp,
        the existing data dict, and the casid of the data currently stored
        in memcache.

        It returns the number of items that were successfully deleted.
        """
        if not data:
            raise CollectionNotFoundError
        if data["modified"] >= modified:
            raise ConflictError
        num_deleted = 0
        for id in items:
            if data["items"].pop(id, None) is not None:
                num_deleted += 1
        if num_deleted > 0:
            data["modified"] = modified
        key = self.get_key(userid)
        if not self.cache.cas(key, data, casid):
            raise ConflictError
        return num_deleted

    #
    # Methods whose implementation can be shared between subclasses.
    #

    def get_timestamp(self, userid):
        data, _ = self.get_cached_data(userid)
        if data is None:
            raise CollectionNotFoundError
        return data["modified"]

    def get_items(self, userid, **kwds):
        # Decode kwds into individual filter values.
        newer = kwds.pop("newer", None)
        limit = kwds.pop("limit", None)
        offset = kwds.pop("offset", None)
        sort = kwds.pop("sort", None)
        ids = kwds.pop("ids", None)
        for unknown_kwd in kwds:
            raise TypeError("Unknown keyword argument: %s" % (unknown_kwd,))
        # Read all the items out of the cache.
        data, _ = self.get_cached_data(userid)
        if data is None:
            raise CollectionNotFoundError
        # Restrict to certain item ids if specified.
        bsos_by_id = data["items"]
        if ids is not None:
            bsos = (bsos_by_id[item] for item in ids if item in bsos_by_id)
        else:
            bsos = bsos_by_id.itervalues()
        # Apply the various filters as generator expressions.
        if newer is not None:
            bsos = (bso for bso in bsos if bso["modified"] > newer)
        # Filter out any that have expired.
        bsos = self._filter_expired_items(bsos)
        # Sort the resulting list.
        # We always sort so that offset/limit work correctly.
        # Using the id as a secondary key produces a unique ordering.
        bsos = list(bsos)
        if sort == "index":
            reverse = True
            key = bso_sort_key_index
        else:
            reverse = False if sort == "oldest" else True
            key = bso_sort_key_modified
        bsos.sort(key=key, reverse=reverse)
        # Trim to the specified offset, if any.
        # Note that we defaulted it to zero above.
        if offset is not None:
            try:
                offset = int(offset)
            except ValueError:
                raise InvalidOffsetError(offset)
            bsos = bsos[offset:]
        # Trim to the specified limit, if any.
        next_offset = None
        if limit is not None:
            if limit < len(bsos):
                bsos = bsos[:limit]
                next_offset = (offset or 0) + limit
        # Return the necessary information.
        return {
            "items": bsos,
            "next_offset": next_offset
        }

    def _filter_expired_items(self, bsos):
        now = int(time.time())
        for bso in bsos:
            ttl = bso.get("ttl")
            if ttl is None or ttl > now:
                yield bso

    def get_item_ids(self, userid, **kwds):
        res = self.get_items(userid, **kwds)
        res["items"] = [bso["id"] for bso in res["items"]]
        return res

    def get_item(self, userid, item):
        items = self.get_items(userid, ids=[item])["items"]
        if not items:
            raise ItemNotFoundError
        return items[0]

    def get_item_timestamp(self, userid, item):
        return self.get_item(userid, item)["modified"]


class CacheOnlyManager(_CachedManagerBase):
    """Object for managing storage of a collection solely in memcached.

    This manager class stores collection data in memcache without writing
    it through to the underlying store.  It manages its own timestamps
    internally and uses CAS to avoid conflicting writes.
    """

    def get_cached_data(self, userid):
        return self.cache.gets(self.get_key(userid))

    def set_items(self, userid, items):
        modified = get_timestamp()
        data, casid = self.get_cached_data(userid)
        self._set_items(userid, items, modified, data, casid)
        return modified

    def del_collection(self, userid):
        if not self.cache.delete(self.get_key(userid)):
            raise CollectionNotFoundError
        return get_timestamp()

    def del_items(self, userid, items):
        modified = get_timestamp()
        data, casid = self.get_cached_data(userid)
        self._del_items(userid, items, modified, data, casid)
        return data["modified"]

    def set_item(self, userid, item, bso):
        bso["id"] = item
        modified = get_timestamp()
        data, casid = self.get_cached_data(userid)
        num_created = self._set_items(userid, [bso], modified, data, casid)
        return {
            "created": num_created == 1,
            "modified": modified,
        }

    def del_item(self, userid, item):
        modified = get_timestamp()
        data, casid = self.get_cached_data(userid)
        num_deleted = self._del_items(userid, [item], modified, data, casid)
        if num_deleted == 0:
            raise ItemNotFoundError
        return modified

    def get_batches_key(self, userid):
        return _key(userid, "c", self.collection, "batches")

    def get_cached_batches(self, userid):
        return self.cache.gets(self.get_batches_key(userid))

    def create_batch(self, userid):
        bdata, bcasid = self.get_cached_batches(userid)
        batch = get_timestamp()
        batchid = int(batch * 1000)
        if not bdata:
            bdata = {}
        if batchid in bdata:
            raise ConflictError
        bdata[batchid] = {"user": userid,
                          "modified": batch,
                          # FIXME Rough guesstimate of the maximum
                          #       reasonable life span of a batch
                          "expires": int(batch) + 2 * 3600,
                          "items": []}
        key = self.get_batches_key(userid)
        if not self.cache.cas(key, bdata, bcasid):
            raise ConflictError
        return batchid

    def valid_batch(self, userid, batch):
        ts = get_timestamp()
        batchid = str(batch)
        bdata, bcasid = self.get_cached_batches(userid)

        if batchid not in bdata or bdata[batchid]["expires"] < ts:
            self.close_batch(userid, batchid)
            return False
        return True

    def append_items_to_batch(self, userid, batch, items):
        modified = get_timestamp()
        batchid = str(batch)
        bdata, bcasid = self.get_cached_batches(userid)
        # Invalid, closed, or expired batch
        if (not bdata or
                batchid not in bdata or
                bdata[batchid]["expires"] <= int(modified)):
            raise InvalidBatch(batch, modified, bdata)

        bdata[batchid]["items"].extend(items)
        bdata[batchid]["modified"] = modified
        key = self.get_batches_key(userid)
        if not self.cache.cas(key, bdata, bcasid):
            raise ConflictError
        return modified

    def apply_batch(self, userid, batch):
        modified = get_timestamp()
        batchid = str(batch)
        bdata, bcasid = self.get_cached_batches(userid)
        # Invalid, closed, or expired batch
        if (not bdata or
                batchid not in bdata or
                bdata[batchid]["expires"] <= int(modified)):
            raise InvalidBatch(batch, modified, bdata)

        data, casid = self.get_cached_data(userid)
        self._set_items(userid, bdata[batchid]["items"], modified, data, casid)
        return modified

    def close_batch(self, userid, batch):
        batchid = str(batch)
        bdata, bcasid = self.get_cached_batches(userid)
        key = self.get_batches_key(userid)

        if batchid in bdata:
            try:
                del bdata[batchid]
            except KeyError:
                return
        if not self.cache.cas(key, bdata, bcasid):
            raise ConflictError


class CachedManager(_CachedManagerBase):
    """Object for managing storage of a collection in both cache and store.

    This manager class duplicates collection data from the underlying store
    into memcache, allowing faster access while guarding against data loss
    in the case of memcache failure/purge.

    To avoid the cache getting out of sync with the underlying store, the
    cached data is deleted before any write operations and restored once
    they are known to have completed.  If something goes wrong, the cache
    data can be restored on next read from the known-good data in the
    underlying store.
    """

    def get_cached_data(self, userid, refresh_if_missing=True):
        """Get the cached collection data, pulling into cache if missing.

        This method returns the cached collection data, populating it from
        the underlying store if it is not cached.
        """
        key = self.get_key(userid)
        data, casid = self.cache.gets(key)
        if data is None and refresh_if_missing:
            data = {}
            try:
                storage = self.storage
                collection = self.collection
                ttl_base = int(get_timestamp())
                with self.owner.lock_for_read(userid, collection):
                    ts = storage.get_collection_timestamp(userid, collection)
                    data["modified"] = ts
                    data["items"] = {}
                    for bso in storage.get_items(userid, collection)["items"]:
                        if bso.get("ttl") is not None:
                            bso["ttl"] = ttl_base + bso["ttl"]
                        data["items"][bso["id"]] = bso
                self.cache.add(key, data)
                data, casid = self.cache.gets(key)
            except CollectionNotFoundError:
                data = None
        return data, casid

    def set_items(self, userid, items):
        storage = self.storage
        # Leave the cache empty if any of posted bsos were missing a payload.
        # This will cause us to lazily read in the defaults from the db.
        refresh_if_missing = True
        for item in items:
            if "payload" not in item:
                refresh_if_missing = False
                break
        with self._mark_dirty(userid, refresh_if_missing) as (data, casid):
            ts = storage.set_items(userid, self.collection, items)
        # Update the cached data in-place to reflect the changes.
        if refresh_if_missing:
            self._set_items(userid, items, ts, data, casid)
        return ts

    def del_collection(self, userid):
        self.cache.delete(self.get_key(userid))
        return self.storage.delete_collection(userid, self.collection)

    def del_items(self, userid, items):
        storage = self.storage
        with self._mark_dirty(userid) as (data, casid):
            ts = storage.delete_items(userid, self.collection, items)
        # Update the cached data, if there was any present.
        if data is not None:
            self._del_items(userid, items, ts, data, casid)
        return ts

    def set_item(self, userid, item, bso):
        storage = self.storage
        # Leave the cache empty if the posted bso was missing a payload.
        # This will cause us to lazily read in the defaults from the db.
        refresh_if_missing = True
        if "payload" not in bso:
            refresh_if_missing = False
        with self._mark_dirty(userid, refresh_if_missing) as (data, casid):
            res = storage.set_item(userid, self.collection, item, bso)
        # Update the cached data in-place to reflect the change.
        if refresh_if_missing:
            bso["id"] = item
            self._set_items(userid, [bso], res["modified"], data, casid)
        return res

    def del_item(self, userid, item):
        storage = self.storage
        with self._mark_dirty(userid) as (data, casid):
            ts = storage.delete_item(userid, self.collection, item)
        # Update the cached data, if there was any present.
        if data is not None:
            self._del_items(userid, [item], ts, data, casid)
        return ts

    def create_batch(self, userid):
        return self.storage.create_batch(userid, self.collection)

    def valid_batch(self, userid, batchid):
        return self.storage.valid_batch(userid, self.collection, batchid)

    def append_items_to_batch(self, userid, batchid, items):
        # Since the items do not appear in the collection until we
        # apply the batch, we don't need to mark anything dirty here.
        return self.storage.append_items_to_batch(userid, self.collection,
                                                  batchid, items)

    def apply_batch(self, userid, batchid):
        # Applying the batch will render our cached data inaccurate.
        # Just leave it emptied, and lazily re-populate on next fetch.
        storage = self.storage
        with self._mark_dirty(userid):
            ts = storage.apply_batch(userid, self.collection, batchid)
        return ts

    def close_batch(self, userid, batchid):
        storage = self.storage
        storage.close_batch(userid, self.collection, batchid)

    @contextlib.contextmanager
    def _mark_dirty(self, userid, refresh_if_missing=False):
        """Context manager to temporarily remove the cached data during write.

        All operations that may modify the underlying collection should be
        performed within this context manager.  It removes the data from cache
        before attempting the write, and rolls back to the old data if
        it is safe to do so.

        Once the write operation has successfully completed, the calling code
        should update the cache with the new data.
        """
        # Grab the current cache state so we can pass it to calling function.
        key = self.get_key(userid)
        data, casid = self.get_cached_data(userid, refresh_if_missing)
        # Remove it from the cache so that we don't serve stale data.
        # A CAS-DELETE here would be nice, but memcached doesn't have one.
        if data is not None:
            self.cache.delete(key)
        # Yield control back the the calling function.
        # Since we've deleted the data, it should always use casid=None.
        try:
            yield data, None
        except StorageError:
            # If they get a storage-related error, it's safe to rollback
            # the cache. For any other sort of error we leave the cache clear.
            if data is not None:
                self.cache.add(key, data)
            raise

    def _set_items(self, userid, *args):
        """Update cached data with new items, or clear it on conflict.

        This method extends the base class _set_items method so that any
        failures are not bubbled up to the calling code.  By the time this
        method is called the write has already succeeded in the underlying
        store, so instead of reporting an error because of the cache, we
        just clear the cached data and let it re-populate on demand.
        """
        try:
            return super(CachedManager, self)._set_items(userid, *args)
        except StorageError:
            self.cache.delete(self.get_key(userid))

    def _del_items(self, userid, *args):
        """Update cached data with deleted items, or clear it on conflict.

        This method extends the base class _del_items method so that any
        failures are not bubbled up to the calling code.  By the time this
        method is called the write has already succeeded in the underlying
        store, so instead of reporting an error because of the cache, we
        just clear the cached data and let it re-populate on demand.
        """
        try:
            return super(CachedManager, self)._del_items(userid, *args)
        except StorageError:
            self.cache.delete(self.get_key(userid))
