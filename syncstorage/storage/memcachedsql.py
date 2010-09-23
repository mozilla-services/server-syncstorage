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
- The meta/global wbo is stored in "user_id
"""
from time import time

from memcache import Client
from sqlalchemy.sql import select, bindparam, func

from syncstorage.storage.sql import SQLStorage
from syncstorage.storage.sqlmappers import wbo

_SQLURI = 'mysql://sync:sync@localhost/sync'
_KB = float(1024)
_COLLECTION_LIST = select([wbo.c.collection, func.max(wbo.c.modified),
                           func.count(wbo)],
            wbo.c.username == bindparam('user_id')).group_by(wbo.c.collection)


def _key(*args):
    return ':'.join([str(arg) for arg in args])


class CacheManager(Client):
    """ Helpers on the top of memcached.Client
    """
    def get_set(self, key, func):
        res = self.get(key)
        if res is None:
            res = func()
            self.set(key, res)
        return res

    def get_tab(self, user_id, tab_id):
        tabs = self.get_tabs(user_id)
        return tabs[tab_id]

    def get_tabs(self, user_id):
        key = _key(user_id, 'tabs')
        return self.get(key)

    def set_tabs(self, user_id, tabs):
        key = _key(user_id, 'tabs')
        existing_tabs = self.get(key)
        if existing_tabs is None:
            existing_tabs = {}
        for tab in tabs:
            existing_tabs[tab['id']] = tab
        self.set(key, existing_tabs)

    def delete_tab(self, user_id, tab_id):
        key = _key(user_id, 'tabs')
        tabs = self.get_tabs(user_id)
        del tabs[tab_id]
        self.set(key, tabs)

    def delete_tabs(self, user_id):
        key = _key(user_id, 'tabs')
        return self.delete(key)

    def tab_exists(self, user_id, tab_id):
        tabs = self.get_tabs(user_id)
        if tab_id in tabs:
            return tabs[tab_id]['modified']
        return None


class MemcachedSQLStorage(SQLStorage):
    """Uses Memcached when possible/useful, SQL otherwise.
    """

    def __init__(self, sqluri=_SQLURI, standard_collections=False,
                 use_quota=False, quota_size=0, pool_size=100,
                 pool_recycle=3600, servers='127.0.01:11211'):
        self.sqlstorage = super(MemcachedSQLStorage, self)
        self.sqlstorage.__init__(sqluri, standard_collections,
                                 use_quota, quota_size, pool_size,
                                 pool_recycle)
        self.cache = CacheManager(servers.split(','))

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
            self.cache.set(_key(user_id, 'size'), total_size)

        # update the meta/global cache or the tabs cache
        if self._is_meta_global(collection_name, items[0]['id']):
            item = items[0]
            item['username'] = user_id
            key = _key(user_id, 'meta', 'global')
            self.cache.set(key, item)
        elif collection_name == 'tabs':
            self.cache.set_tabs(user_id, items)
            # update the timestamp cache
            key = _key(user_id, 'collections', 'stamp', 'tabs')
            self.cache.set(key, time())

    def _update_item(self, item, when):
        if self.use_quota and 'payload' in item:
            item['payload_size'] = len(item['payload'])
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
        # delete the cached size
        self.cache.delete(_key(user_id, 'size'))

        # remove the cached values
        if (collection_name == 'meta' and (item_ids is None
            or 'global' in item_ids)):
            key = _key(user_id, 'meta', 'global')
            self.cache.delete(key)
        elif collection_name == 'tabs':
            self.cache.delete_tabs(user_id)
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
            self.cache.set(_key(user_id, 'size'), size)
            return size

        size = self.cache.get(_key(user_id, 'size'))
        if size is None:
            return 0.
        return  size / _KB

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.
        """
        sizes = self.sqlstorage.get_collection_sizes(user_id)
        tabs_size = 0

        # xxx
        #for item_id in self._conn.smembers(_key('tabs', user_id)):
        #    tab_size = self._conn.get(_key('tabs', 'size', user_id, item_id))
        #    if tab_size is not None:
        #        tabs_size += int(tab_size)

        sizes['tabs'] = tabs_size
        return sizes

    def get_collection_timestamps(self, user_id):
        stamps = self.cache.get(_key(user_id, 'stamps'))

        if stamps is None:
            # not cached yet
            stamps = super(MemcachedSQLStorage,
                           self).get_collection_timestamps(user_id)

            # caching it
            self.cache.set(_key(user_id, 'stamps'), stamps)

            # we also need to add the tabs timestamp
            key = _key(user_id, 'collections', 'stamp', 'tabs')
            stamps['tabs'] = self.cache.get(key)

            # cache marker
            self.cache.set(_key(user_id, 'stamps'), stamps)
            return stamps

        # we also need to add the tabs timestamp
        key = _key(user_id, 'collections', 'stamp', 'tabs')
        stamps['tabs'] = self.cache.get(key)
        return stamps

    def get_collection_max_timestamp(self, user_id, collection_name):
        # let's get them all, so they get cached
        stamps = self.get_collection_timestamps(user_id)
        return stamps[collection_name]