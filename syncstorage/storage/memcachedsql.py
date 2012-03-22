# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Memcached + SQL backend

- User tabs are stored in one single "user_id:tabs" key
- The total storage size is stored in "user_id:size"
- The meta/global bso is stored in "user_id"
"""
import simplejson as json

from sqlalchemy.sql import select, bindparam, func

from mozsvc.exceptions import BackendError

from syncstorage import logger
from syncstorage.util import get_timestamp
from syncstorage.storage.sql import SQLStorage, _KB
from syncstorage.storage.sqlmappers import bso
from syncstorage.storage.cachemanager import CacheManager


_COLLECTION_LIST = select([bso.c.collection, func.max(bso.c.modified),
                           func.count(bso)],
            bso.c.userid == bindparam('user_id')).group_by(bso.c.collection)


def _key(*args):
    return ':'.join([str(arg) for arg in args])


# XXX suboptimal: creates an object on every dump/load call
# but that's how python-memcached works - using a class
# instead of an object would not be thread-safe.
#
# Need to ask Sean to improve this
class _JSONDumper(object):
    """Dumps and loads json in a file-like object"""
    def __init__(self, file, protocol=0):
        self.file = file

    def dump(self, val):
        self.file.write(json.dumps(val))

    def load(self):
        return json.loads(self.file.read())


class MemcachedSQLStorage(SQLStorage):
    """Uses Memcached when possible/useful, SQL otherwise.
    """

    def __init__(self, sqluri, standard_collections=False,
                 use_quota=False, quota_size=0, pool_size=100,
                 pool_recycle=3600, cache_servers=None,
                 create_tables=False, shard=False, shardsize=100,
                 memcached_json=False, **kw):
        self.sqlstorage = super(MemcachedSQLStorage, self)
        self.sqlstorage.__init__(sqluri, standard_collections,
                                 use_quota, quota_size, pool_size,
                                 pool_recycle, create_tables=create_tables,
                                 shard=shard, shardsize=shardsize, **kw)
        if isinstance(cache_servers, str):
            cache_servers = [cache_servers]
        elif cache_servers is None:
            cache_servers = ['127.0.0.1:11211']
        extra_kw = {}
        if memcached_json:
            extra_kw['pickler'] = _JSONDumper
            extra_kw['unpickler'] = _JSONDumper
        self.cache = CacheManager(cache_servers, **extra_kw)

    @classmethod
    def get_name(self):
        return 'memcached'

    #
    # Cache management
    #
    def _is_meta_global(self, collection_name, item_id):
        return collection_name == 'meta' and item_id == 'global'

    #
    # Cached APIs
    #
    def delete_storage(self, user_id):
        self.cache.flush_user_cache(user_id)
        self.sqlstorage.delete_storage(user_id)

    def item_exists(self, user_id, collection_name, item_id):
        """Returns a timestamp if an item exists."""
        def _item_exists():
            return self.sqlstorage.item_exists(user_id, collection_name,
                                               item_id)

        # returning cached values when possible
        if self._is_meta_global(collection_name, item_id):
            key = _key(user_id, 'meta', 'global')
            bso = self.cache.get(key)
            if bso is not None:
                return bso['modified']
            # going sql..

        elif collection_name == 'tabs':
            return self.cache.tab_exists(user_id, item_id)

        return self.sqlstorage.item_exists(user_id, collection_name, item_id)

    def get_items(self, user_id, collection_name, fields=None, filters=None,
                  limit=None, sort=None):
        """returns items from a collection

        "filter" is a dict used to add conditions to the db query.
        Its keys are the field names on which the condition operates.
        Its values are the values the field should have.
        It can be a single value, or a list. For the latter the in()
        operator is used. For single values, the operator has to be provided.
        """
        # returning cached values when possible
        if collection_name == 'tabs':
            # tabs are not stored at all in SQL
            return self.cache.get_tabs(user_id, filters).values()

        return self.sqlstorage.get_items(user_id, collection_name,
                                         fields, filters, limit, sort)

    def get_item(self, user_id, collection_name, item_id, fields=None):
        """Returns one item.

        If the item is meta/global, we want to get the cached one if present.
        """
        def _get_item():
            return self.sqlstorage.get_item(user_id, collection_name,
                                            item_id, fields)

        # returning cached values when possible
        if self._is_meta_global(collection_name, item_id):
            key = _key(user_id, 'meta', 'global')
            return self.cache.get_set(key, _get_item)
        elif collection_name == 'tabs':
            # tabs are not stored at all in SQL
            return self.cache.get_tab(user_id, item_id)

        return _get_item()

    def _delete_cache(self, user_id):
        """Removes all cached data."""
        for key in ('size', 'meta:global', 'tabs'):
            self.cache.delete(_key(user_id, key))

    def _update_stamp(self, user_id, collection_name, storage_time):
        # update the stamps cache
        if storage_time is None:
            storage_time = get_timestamp()
        stamps = self.get_collection_timestamps(user_id)
        stamps[collection_name] = storage_time
        self.cache.set(_key(user_id, 'stamps'), stamps)

    def _update_cache(self, user_id, collection_name, items, storage_time):
        # update the total size cache (bytes)
        total_size = sum([len(item.get('payload', '')) for item in items])
        self.cache.incr(_key(user_id, 'size'), total_size)

        # update the stamps cache
        self._update_stamp(user_id, collection_name, storage_time)

        # update the meta/global cache or the tabs cache
        if self._is_meta_global(collection_name, items[0]['id']):
            item = items[0]
            item['userid'] = user_id
            key = _key(user_id, 'meta', 'global')
            self.cache.set(key, item)
        elif collection_name == 'tabs':
            tabs = dict([(item['id'], item) for item in items])
            self.cache.set_tabs(user_id, tabs)

    def _update_item(self, item, when):
        if 'payload' in item:
            item['modified'] = when

    def set_item(self, user_id, collection_name, item_id, storage_time=None,
                 **values):
        """Adds or update an item"""
        values['id'] = item_id
        if storage_time is None:
            storage_time = get_timestamp()

        self._update_item(values, storage_time)
        self._update_cache(user_id, collection_name, [values], storage_time)

        if collection_name == 'tabs':
            # return now : we don't store tabs in sql
            return storage_time

        return self.sqlstorage.set_item(user_id, collection_name, item_id,
                                        storage_time=storage_time, **values)

    def set_items(self, user_id, collection_name, items, storage_time=None):
        """Adds or update a batch of items.

        Returns a list of success or failures.
        """
        if storage_time is None:
            storage_time = get_timestamp()

        for item in items:
            self._update_item(item, storage_time)

        if items:
            self._update_cache(user_id, collection_name, items, storage_time)

        if collection_name == 'tabs':
            # return now : we don't store tabs in sql
            return len(items)

        return self.sqlstorage.set_items(user_id, collection_name, items,
                                         storage_time=storage_time)

    def delete_item(self, user_id, collection_name, item_id,
                    storage_time=None):
        """Deletes an item"""
        # delete the cached size - will be recalculated
        self.cache.delete(_key(user_id, 'size'))

        # update the meta/global cache or the tabs cache
        if self._is_meta_global(collection_name, item_id):
            key = _key(user_id, 'meta', 'global')
            self.cache.delete(key)
        elif collection_name == 'tabs':
            # tabs are not stored at all in SQL
            if self.cache.delete_tab(user_id, item_id):
                self._update_stamp(user_id, 'tabs', storage_time)
                return True
            return False

        res = self.sqlstorage.delete_item(user_id, collection_name, item_id)
        if res:
            self._update_stamp(user_id, collection_name, storage_time)
        return res

    def delete_items(self, user_id, collection_name, item_ids=None,
                     storage_time=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        # delete the cached size
        self.cache.delete(_key(user_id, 'size'))

        # remove the cached values
        if (collection_name == 'meta' and (item_ids is None
            or 'global' in item_ids)):
            key = _key(user_id, 'meta', 'global')
            self.cache.delete(key)
        elif collection_name == 'tabs':
            # tabs are not stored at all in SQL
            if self.cache.delete_tabs(user_id, item_ids):
                self._update_stamp(user_id, 'tabs', storage_time)
                return True
            return False

        res = self.sqlstorage.delete_items(user_id, collection_name,
                                           item_ids, storage_time)
        if res:
            self._update_stamp(user_id, collection_name, storage_time)
        return res

    def _set_cached_size(self, user_id, size):
        key = _key(user_id, 'size')
        # if this fail it's not a big deal
        try:
            # we store the size in bytes in memcached
            self.cache.set(key, size * _KB)
        except BackendError:
            logger.error('Could not write to memcached')

    def _get_cached_size(self, user_id):
        try:
            size = self.cache.get(_key(user_id, 'size'))
            if size != 0 and size is not None:
                size = size / _KB
        except BackendError:
            size = None
        return size

    def get_total_size(self, user_id, recalculate=False):
        """Returns the total size in KB of a user storage"""
        def _get_set_size():
            # returns in KB
            size = self.sqlstorage.get_total_size(user_id)

            # adding the tabs
            size += self.cache.get_tabs_size(user_id)

            # update the cache
            self.cache.set_total(user_id, size)
            return size

        if recalculate:
            return _get_set_size()

        size = self.cache.get_total(user_id)
        if not size:    # memcached server seems down or needs a reset
            return _get_set_size()

        return  size

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.
        """
        # these sizes are not cached
        sizes = self.sqlstorage.get_collection_sizes(user_id)
        sizes['tabs'] = self.cache.get_tabs_size(user_id)

        # we can update the size while we're there, in case it's empty
        self.cache.set_total(user_id, sum(sizes.values()))
        return sizes

    def get_collection_timestamps(self, user_id):
        """Returns a cached version of the stamps when possible"""
        stamps = self.cache.get(_key(user_id, 'stamps'))

        # not cached yet or memcached is down
        if stamps is None:
            stamps = super(MemcachedSQLStorage,
                           self).get_collection_timestamps(user_id)

            # adding the tabs stamp
            tabs_stamps = self.cache.get_tabs_timestamp(user_id)
            if tabs_stamps is not None:
                stamps['tabs'] = tabs_stamps

            # caching it
            self.cache.set(_key(user_id, 'stamps'), stamps)

        return stamps

    def get_collection_max_timestamp(self, user_id, collection_name):
        # let's get them all, so they get cached
        stamps = self.get_collection_timestamps(user_id)
        if collection_name == 'tabs' and 'tabs' not in stamps:
            return None
        return stamps[collection_name]
