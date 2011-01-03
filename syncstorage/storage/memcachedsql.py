# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Sync Server
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2010
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Tarek Ziade (tarek@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
"""
Memcached + SQL backend

- User tabs are stored in one single "user_id:tabs" key
- The total storage size is stored in "user_id:size"
- The meta/global wbo is stored in "user_id"
"""
from time import time
import threading
import json

from memcache import Client
from sqlalchemy.sql import select, bindparam, func

from syncstorage.storage.sql import SQLStorage, _KB
from syncstorage.storage.sqlmappers import wbo


_COLLECTION_LIST = select([wbo.c.collection, func.max(wbo.c.modified),
                           func.count(wbo)],
            wbo.c.username == bindparam('user_id')).group_by(wbo.c.collection)


def _key(*args):
    return ':'.join([str(arg) for arg in args])


class CacheManager(Client):
    """ Helpers on the top of memcached.Client
    """
    def __init__(self, *args, **kw):
        Client.__init__(self, *args, **kw)
        # using a locker to avoid race conditions
        # when several clients for the same user
        # get/set the cached data
        self._locker = threading.RLock()

    def get_set(self, key, func):
        res = self.get(key)
        if res is None:
            res = func()
            self.set(key, res)
        return res

    def get_tab(self, user_id, tab_id):
        tabs = self.get_tabs(user_id)
        if tabs is None:
            return None
        return tabs.get(tab_id)

    def get_tabs(self, user_id, filters=None):
        with self._locker:
            key = _key(user_id, 'tabs')
            tabs = self.get(key)
            if tabs is None:
                # memcached down ?
                tabs = {}
            if filters is not None:
                if 'id' in filters:
                    operator, ids = filters['id']
                    if operator == 'in':
                        for tab_id in list(tabs.keys()):
                            if tab_id not in ids:
                                del tabs[tab_id]
                if 'modified' in filters:
                    operator, stamp = filters['modified']
                    if operator == '>':
                        for tab_id, tab in list(tabs.items()):
                            if tab['modified'] <= stamp:
                                del tabs[tab_id]
                    elif operator == '<':
                        for tab_id, tab in list(tabs.items()):
                            if tab['modified'] >= stamp:
                                del tabs[tab_id]
                if 'sortindex' in filters:
                    operator, stamp = filters['sortindex']
                    if operator == '>':
                        for tab_id, tab in list(tabs.items()):
                            if tab['sortindex'] <= stamp:
                                del tabs[tab_id]
                    elif operator == '<':
                        for tab_id, tab in list(tabs.items()):
                            if tab['sortindex'] >= stamp:
                                del tabs[tab_id]
            return tabs

    def set_tabs(self, user_id, tabs, merge=True):
        with self._locker:
            key = _key(user_id, 'tabs')
            if merge:
                existing_tabs = self.get(key)
                if existing_tabs is None:
                    existing_tabs = {}
            else:
                existing_tabs = {}
            for tab_id, tab in tabs.items():
                existing_tabs[tab_id] = tab
            self.set(key, existing_tabs)

    def delete_tab(self, user_id, tab_id):
        with self._locker:
            key = _key(user_id, 'tabs')
            tabs = self.get_tabs(user_id)
            del tabs[tab_id]
            self.set(key, tabs)

    def delete_tabs(self, user_id, filters=None):
        with self._locker:
            key = _key(user_id, 'tabs')
            kept = {}
            tabs = self.get(key)
            if tabs is None:
                # memcached down ?
                tabs = {}

            if filters is not None:
                if 'id' in filters:
                    operator, ids = filters['id']
                    if operator == 'in':
                        for tab_id in list(tabs.keys()):
                            if tab_id not in ids:
                                kept[tab_id] = tabs[tab_id]
                if 'modified' in filters:
                    operator, stamp = filters['modified']
                    if operator == '>':
                        for tab_id, tab in list(tabs.items()):
                            if tab['modified'] <= stamp:
                                kept[tab_id] = tabs[tab_id]
                    elif operator == '<':
                        for tab_id, tab in list(tabs.items()):
                            if tab['modified'] >= stamp:
                                kept[tab_id] = tabs[tab_id]
                if 'sortindex' in filters:
                    operator, stamp = filters['sortindex']
                    if operator == '>':
                        for tab_id, tab in list(tabs.items()):
                            if tab['sortindex'] <= stamp:
                                kept[tab_id] = tabs[tab_id]
                    elif operator == '<':
                        for tab_id, tab in list(tabs.items()):
                            if tab['sortindex'] >= stamp:
                                kept[tab_id] = tabs[tab_id]
            self.set(key, kept)

    def tab_exists(self, user_id, tab_id):
        tabs = self.get_tabs(user_id)
        if tabs is None:
            # memcached down ?
            return None
        if tab_id in tabs:
            return tabs[tab_id]['modified']
        return None


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
                 create_tables=True, shard=False, shardsize=100,
                 memcached_json=False, **kw):
        self.sqlstorage = super(MemcachedSQLStorage, self)
        self.sqlstorage.__init__(sqluri, standard_collections,
                                 use_quota, quota_size, pool_size,
                                 pool_recycle, create_tables=create_tables,
                                 shard=shard, shardsize=shardsize)
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
        #self._delete_cache(user_id)
        # XXX
        self.sqlstorage.delete_storage(user_id)

    def delete_user(self, user_id):
        # XXX
        # self._delete_cache(user_id)
        self.sqlstorage.delete_user(user_id)

    def item_exists(self, user_id, collection_name, item_id):
        """Returns a timestamp if an item exists."""
        def _item_exists():
            return self.sqlstorage.item_exists(user_id, collection_name,
                                               item_id)

        # returning cached values when possible
        if self._is_meta_global(collection_name, item_id):
            key = _key(user_id, 'meta', 'global')
            wbo = self.cache.get(key)
            if wbo is not None:
                return wbo['modified']
            return None

        elif collection_name == 'tabs':
            return self.cache.tab_exists(user_id, item_id)

        return self.sqlstorage.item_exists(user_id, collection_name, item_id)

    def get_items(self, user_id, collection_name, fields=None, filters=None,
                  limit=None, offset=None, sort=None):
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
                                         fields, filters, limit, offset, sort)

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

    def _update_cache(self, user_id, collection_name, items):
        # update the total size cache
        total_size = sum([len(item.get('payload', '')) for item in items])
        if not self.cache.incr(_key(user_id, 'size'), total_size):
            # stored in bytes
            self.cache.set(_key(user_id, 'size'), total_size)

        # update the meta/global cache or the tabs cache
        if self._is_meta_global(collection_name, items[0]['id']):
            item = items[0]
            item['username'] = user_id
            key = _key(user_id, 'meta', 'global')
            self.cache.set(key, item)
        elif collection_name == 'tabs':
            tabs = dict([(item['id'], item) for item in items])
            self.cache.set_tabs(user_id, tabs)
            # update the timestamp cache
            key = _key(user_id, 'collections', 'stamp', 'tabs')
            self.cache.set(key, time())

        # invalidate the stamps cache
        self.cache.delete(_key(user_id, 'stamps'))

    def _update_item(self, item, when):
        if 'payload' in item:
            item['modified'] = when

    def set_item(self, user_id, collection_name, item_id, **values):
        """Adds or update an item"""
        now = time()
        values['id'] = item_id
        self._update_item(values, now)
        self._update_cache(user_id, collection_name, [values])

        if collection_name == 'tabs':
            # return now : we don't store tabs in sql
            return now
        return self.sqlstorage.set_item(user_id, collection_name, item_id,
                                        **values)

    def set_items(self, user_id, collection_name, items):
        """Adds or update a batch of items.

        Returns a list of success or failures.
        """
        now = time()
        for item in items:
            self._update_item(item, now)

        self._update_cache(user_id, collection_name, items)
        if collection_name == 'tabs':
            # return now : we don't store tabs in sql
            return len(items)
        return self.sqlstorage.set_items(user_id, collection_name, items)

    def delete_item(self, user_id, collection_name, item_id):
        """Deletes an item"""
        # delete the cached size
        self.cache.delete(_key(user_id, 'size'))

        # invalidate the stamps cache
        self.cache.delete(_key(user_id, 'stamps'))

        # update the meta/global cache or the tabs cache
        if self._is_meta_global(collection_name, item_id):
            key = _key(user_id, 'meta', 'global')
            self.cache.delete(key)
        elif collection_name == 'tabs':
            self.cache.delete_tab(user_id, item_id)
            key = _key(user_id, 'collections', 'stamp', collection_name)
            self.cache.set(key, time())
            # we don't store tabs in SQL
            return

        return self.sqlstorage.delete_item(user_id, collection_name, item_id)

    def delete_items(self, user_id, collection_name, item_ids=None,
                     filters=None, limit=None, offset=None, sort=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        # delete the cached size and stamps
        self.cache.delete(_key(user_id, 'size'))
        self.cache.delete(_key(user_id, 'stamps'))

        # remove the cached values
        if (collection_name == 'meta' and (item_ids is None
            or 'global' in item_ids)):
            key = _key(user_id, 'meta', 'global')
            self.cache.delete(key)
        elif collection_name == 'tabs':
            # tabs are not stored at all in SQL
            self.cache.delete_tabs(user_id, filters)

            # we don't store tabs in SQL
            # update the timestamp cache
            key = _key(user_id, 'collections', 'stamp', collection_name)
            self.cache.set(key, time())
            return

        return self.sqlstorage.delete_items(user_id, collection_name,
                                            item_ids, filters,
                                            limit, offset, sort)

    def get_total_size(self, user_id, recalculate=False):
        """Returns the total size in KB of a user storage"""
        if recalculate:
            size = self.sqlstorage.get_total_size(user_id)
            self.cache.set(_key(user_id, 'size'), int(size * _KB))
            return size

        size = self.cache.get(_key(user_id, 'size'))
        if size is None:    # memcached server seems down
            size = self.sqlstorage.get_total_size(user_id) * _KB
        return  size / _KB

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.
        """
        sizes = self.sqlstorage.get_collection_sizes(user_id)
        tabs = self.cache.get_tabs(user_id)
        sizes['tabs'] = sum([len(tab.get('payload'), '') for tab in tabs])
        return sizes

    def get_collection_timestamps(self, user_id):
        stamps = self.cache.get(_key(user_id, 'stamps'))

        # not cached yet or memcached is down
        if stamps is None:
            stamps = super(MemcachedSQLStorage,
                           self).get_collection_timestamps(user_id)

            # caching it
            self.cache.set(_key(user_id, 'stamps'), stamps)

        # we also need to add the tabs timestamp
        key = _key(user_id, 'collections', 'stamp', 'tabs')
        tabs_stamp = self.cache.get(key)
        if tabs_stamp is not None:  # memcached down ?
            stamps['tabs'] = tabs_stamp
        return stamps

    def get_collection_max_timestamp(self, user_id, collection_name):
        # let's get them all, so they get cached
        stamps = self.get_collection_timestamps(user_id)
        return stamps[collection_name]
