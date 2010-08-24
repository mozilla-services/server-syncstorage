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
Redis + SQL backend
"""
import json
from time import time

import redis

from weaveserver.storage.sql import WeaveSQLStorage
from weaveserver import logger

_SQLURI = 'mysql://sync:sync@localhost/sync'
_KB = float(1024)


def _key(*args):
    return ':'.join([str(arg) for arg in args])


class GracefulRedisServer(redis.Redis):
    """If the Redis server gets down, we emit log.errors but
    make sure the app does not break"""

    def get(self, key):
        try:
            return super(GracefulRedisServer, self).get(key)
        except redis.client.ConnectionError, e:
            logger.error(str(e))
            return None

    def set(self, key, value):
        try:
            return super(GracefulRedisServer, self).set(key, value)
        except redis.client.ConnectionError, e:
            logger.error(str(e))
            return None

    def smembers(self, key):
        try:
            return super(GracefulRedisServer, self).smembers(key)
        except redis.client.ConnectionError, e:
            logger.error(str(e))
            return []

    def sismember(self, key, value):
        try:
            return super(GracefulRedisServer, self).sismember(key, value)
        except redis.client.ConnectionError, e:
            logger.error(str(e))
            return False

    def srem(self, key, value):
        try:
            super(GracefulRedisServer, self).srem(key, value)
        except redis.client.ConnectionError, e:
            logger.error(str(e))


class RediSQLStorage(WeaveSQLStorage):
    """Uses Redis when possible/useful, SQL otherwise.
    """

    def __init__(self, sqluri=_SQLURI, standard_collections=False,
                 use_quota=False, quota_size=0, pool_size=100,
                 pool_recycle=3600, redis_host='localhost',
                 redis_port=6379):
        super(RediSQLStorage, self).__init__(sqluri, standard_collections,
                                             use_quota, quota_size, pool_size,
                                             pool_recycle)
        self._conn = GracefulRedisServer(host=redis_host, port=redis_port)
        self._conn.ping()  # will generate a connection error if down

    @classmethod
    def get_name(self):
        return 'redisql'

    def _delete_cache(self, user_id):
        """Removes all cache for the given user"""
        item_ids = self._conn.smembers(_key('tabs', user_id))

        for item_id in item_ids:
            self._conn.srem(_key('tabs', user_id), item_id)
            self._conn.set(_key('tabs', user_id, item_id), None)
            self._conn.set(_key('tabs', 'size', user_id, item_id), None)

        self._conn.set(_key('meta', 'global', user_id), None)

    def delete_storage(self, user_id):
        self._delete_cache(user_id)
        super(RediSQLStorage, self).delete_storage(user_id)

    def delete_user(self, user_id):
        self._delete_cache(user_id)
        super(RediSQLStorage, self).delete_user(user_id)

    def _is_meta_global(self, collection_name, item_id):
        return collection_name == 'meta' and item_id == 'global'

    def item_exists(self, user_id, collection_name, item_id):
        """Returns a timestamp if an item exists."""
        if self._is_meta_global(collection_name, item_id):
            value = self._conn.get(_key('meta', 'global', user_id))
            if value is not None:
                wbo = json.loads(value)
                return wbo['modified']
        elif collection_name == 'tabs':
            if self._conn.sismember(_key('tabs', user_id), item_id):
                return True

        return super(RediSQLStorage, self).item_exists(user_id,
                                                       collection_name,
                                                       item_id)

    def get_item(self, user_id, collection_name, item_id, fields=None):
        """Returns one item.

        If the item is meta/global, we want to get the cached one if present.
        """
        if self._is_meta_global(collection_name, item_id):
            value = self._conn.get(_key('meta', 'global', user_id))
            if value is not None:
                return json.loads(value)
        elif collection_name == 'tabs':
            value = self._conn.get(_key('tabs', user_id, item_id))
            if value is not None:
                return json.loads(value)

        return super(RediSQLStorage, self).get_item(user_id, collection_name,
                                                    item_id, fields)

    def set_item(self, user_id, collection_name, item_id, **values):
        """Adds or update an item"""
        if 'payload' in values and 'modified' not in values:
            values['modified'] = time()

        if self._is_meta_global(collection_name, item_id):
            self._conn.set(_key('meta', 'global', user_id),
                           json.dumps(values))
        elif collection_name == 'tabs':
            self._conn.sadd(_key('tabs', user_id), item_id)
            self._conn.set(_key('tabs', user_id, item_id),
                            json.dumps(values))
            self._conn.set(_key('tabs', 'size', user_id, item_id),
                            len(values.get('payload', '')))
            # we don't store tabs in SQL
            return

        return self._set_item(user_id, collection_name, item_id, **values)

    def set_items(self, user_id, collection_name, items):
        """Adds or update a batch of items.

        Returns a list of success or failures.
        """
        if self._is_meta_global(collection_name, items[0]['id']):
            values = items[0]
            values['username'] = user_id
            self._conn.set(_key('meta', 'global', user_id),
                           json.dumps(values))
        elif collection_name == 'tabs':
            for item in items:
                item_id = item['id']
                self._conn.sadd(_key('tabs', user_id), item_id)
                self._conn.set(_key('tabs', user_id, item_id),
                                json.dumps(item))
                self._conn.set(_key('tabs', 'size', user_id, item_id),
                               len(item.get('payload', '')))
            # we don't store tabs in SQL
            return

        return super(RediSQLStorage, self).set_items(user_id, collection_name,
                                                     items)

    def delete_item(self, user_id, collection_name, item_id):
        """Deletes an item"""
        if self._is_meta_global(collection_name, item_id):
            self._conn.set(_key('meta', 'global', user_id), None)
        elif collection_name == 'tabs':
            self._conn.srem(_key('tabs', user_id), item_id)
            self._conn.set(_key('tabs', user_id, item_id), None)
            self._conn.set(_key('tabs', 'size', user_id, item_id), None)
            # we don't store tabs in SQL
            return

        return super(RediSQLStorage, self).delete_item(user_id,
                                                       collection_name,
                                                       item_id)

    def delete_items(self, user_id, collection_name, item_ids=None,
                     filters=None, limit=None, offset=None, sort=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        if (collection_name == 'meta' and (item_ids is None
            or 'global' in item_ids)):
            self._conn.set(_key('meta', 'global', user_id), None)
        elif collection_name == 'tabs':
            # getting all members
            if item_ids is None:
                item_ids = self._conn.smembers(_key('tabs', user_id))

            for item_id in item_ids:
                self._conn.srem(_key('tabs', user_id), item_id)
                self._conn.set(_key('tabs', user_id, item_id), None)
                self._conn.set(_key('tabs', 'size', user_id, item_id), None)

            # we don't store tabs in SQL
            return

        return super(RediSQLStorage, self).delete_items(user_id,
                                                        collection_name,
                                                        item_ids, filters,
                                                        limit, offset, sort)

    def get_total_size(self, user_id):
        """Returns the total size in KB of a user storage"""
        size = super(RediSQLStorage, self).get_total_size(user_id)

        # add the tabs sizes, if any
        tabs_size = 0
        for item_id in self._conn.smembers(_key('tabs', user_id)):
            tab_size = self._conn.get(_key('tabs', 'size', user_id, item_id))
            if tab_size is not None:
                tabs_size += int(tab_size)

        return size + tabs_size / _KB

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.
        """
        sizes = super(RediSQLStorage, self).get_collection_sizes(user_id)
        tabs_size = 0
        for item_id in self._conn.smembers(_key('tabs', user_id)):
            tab_size = self._conn.get(_key('tabs', 'size', user_id, item_id))
            if tab_size is not None:
                tabs_size += int(tab_size)

        sizes['tabs'] = tabs_size
        return sizes
