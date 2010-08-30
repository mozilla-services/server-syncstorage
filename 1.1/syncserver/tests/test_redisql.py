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
import unittest
from collections import defaultdict

from syncserver.storage.redisql import RediSQLStorage
from syncserver.storage import redisql
from syncserver.storage import WeaveStorage

_UID = 1

# manual registration
WeaveStorage.register(RediSQLStorage)


class FakeRedis(dict):

    def __init__(self, host, port):
        self.set_called = self.get_called = 0
        self.sets = defaultdict(list)

    def ping(self):
        pass

    def set(self, name, value):
        if value is None and name in self:
            del self[name]
            return
        self.set_called += 1
        self[name] = value

    def get(self, name):
        self.get_called += 1
        try:
            return self[name]
        except KeyError:
            return None

    def sadd(self, key, name):
        self.sets[key].append(name)

    def sismember(self, key, name):
        return name in self.sets[key]

    def srem(self, key, name):
        self.sets[key].remove(name)

    def smembers(self, key):
        return [id_ for id_ in self.sets[key]]


class TestRediSQLStorage(unittest.TestCase):

    def setUp(self):
        self.old = redisql.GracefulRedisServer
        redisql.GracefulRedisServer = FakeRedis
        self.storage = WeaveStorage.get('redisql',
                                        sqluri='sqlite:///:memory:',
                                        use_quota=True,
                                        quota_size=5120)
        # make sure we have the standard collections in place
        for name in ('client', 'crypto', 'forms', 'history'):
            self.storage.set_collection(_UID, name)

    def tearDown(self):
        self.storage.delete_user(_UID)
        redisql.GracefulRedisServer = self.old

    def test_basic(self):
        # just make sure calls goes through
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col1')
        self.storage.set_item(_UID, 'col1', '1', payload='XXX')

        # these calls should be cached
        res = self.storage.get_item(_UID, 'col1', '1')
        self.assertEquals(res['payload'], 'XXX')

        # this should remove the cache
        self.storage.delete_items(_UID, 'col1')
        items = self.storage.get_items(_UID, 'col1')
        self.assertEquals(len(items), 0)

    def test_meta_global(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'meta')
        self.storage.set_item(_UID, 'meta', 'global', payload='XXX')

        # these calls should be cached
        res = self.storage.get_item(_UID, 'meta', 'global')
        self.assertEquals(res['payload'], 'XXX')
        self.assertEquals(self.storage._conn.get_called, 2)
        self.assertEquals(self.storage._conn.set_called, 2)
        self.assertEquals(self.storage._conn.keys(), ['meta:global:1',
                                                  'collections:stamp:1:meta'])

        # this should remove the cache
        self.storage.delete_item(_UID, 'meta', 'global')
        self.assertEquals(self.storage._conn.keys(),
                          ['collections:stamp:1:meta'])

        items = [{'id': 'global', 'payload': 'xxx'},
                 {'id': 'other', 'payload': 'xxx'},
                ]
        self.storage.set_items(_UID, 'meta', items)
        self.assertEquals(self.storage._conn.keys(), ['meta:global:1',
                                                  'collections:stamp:1:meta'])

        # this should remove the cache
        self.storage.delete_items(_UID, 'meta')
        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 0)
        self.assertEquals(self.storage._conn.keys(),
                          ['collections:stamp:1:meta'])

    def test_tabs(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'tabs')
        self.storage.set_item(_UID, 'tabs', '1', payload='XXX')

        # these calls should be cached
        res = self.storage.get_item(_UID, 'tabs', '1')
        self.assertEquals(res['payload'], 'XXX')
        self.assertEquals(self.storage._conn.get_called, 1)
        self.assertEquals(self.storage._conn.set_called, 3)
        self.assertEquals(self.storage._conn.keys(), ['tabs:1:1',
                                                  'tabs:size:1:1',
                                                  'collections:stamp:1:tabs'])

        # this should remove the cache
        self.storage.delete_item(_UID, 'tabs', '1')
        self.assertEquals(self.storage._conn.keys(),
                          ['collections:stamp:1:tabs'])

        items = [{'id': '1', 'payload': 'xxx'},
                 {'id': '2', 'payload': 'xxx'},
                ]
        self.storage.set_items(_UID, 'tabs', items)
        keys = self.storage._conn.keys()
        keys.sort()
        self.assertEquals(keys, ['collections:stamp:1:tabs', 'tabs:1:1',
                                 'tabs:1:2', 'tabs:size:1:1',
                                 'tabs:size:1:2'])

        # this should remove the cache
        self.storage.delete_items(_UID, 'tabs')
        items = self.storage.get_items(_UID, 'tabs')
        self.assertEquals(len(items), 0)
        self.assertEquals(self.storage._conn.keys(),
                          ['collections:stamp:1:tabs'])

    def test_size(self):
        # make sure we get the right size
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'tabs')
        self.storage.set_collection(_UID, 'foo')
        self.storage.set_item(_UID, 'tabs', '1', payload='XXX' * 200)
        self.storage.set_item(_UID, 'foo', '1', payload='XXX' * 200)

        wanted = (len('XXX' * 200) * 2) / 1024.
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

    def test_collection_stamps(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'tabs')
        self.storage.set_collection(_UID, 'foo')
        self.storage.set_item(_UID, 'tabs', '1', payload='XXX' * 200)
        self.storage.set_item(_UID, 'foo', '1', payload='XXX' * 200)

        get = self.storage._conn.get_called
        set = self.storage._conn.set_called
        keys = self.storage._conn.keys()

        stamps = self.storage.get_collection_timestamps(_UID)  # pumping cache
        stamps2 = self.storage.get_collection_timestamps(_UID)
        self.assertEquals(len(stamps), len(stamps2))
        self.assertEquals(len(stamps), 6)
        self.assertEquals(self.storage._conn.get_called, get + 9)
        self.assertEquals(self.storage._conn.set_called, set + 7)
        self.assertEquals(len(self.storage._conn.keys()), len(keys) + 5)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestRediSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
