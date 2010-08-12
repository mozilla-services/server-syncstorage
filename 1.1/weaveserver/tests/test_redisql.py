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
import redis

from weaveserver.storage.redisql import RediSQLStorage
from weaveserver.storage import redisql
from weaveserver.storage import WeaveStorage

_UID = 1

# manual registration
WeaveStorage.register(RediSQLStorage)


class FakeRedis(dict):

    def __init__(self, host, port):
        self.set_called = self.get_called = 0

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
        return self[name]


class TestRediSQLStorage(unittest.TestCase):

    def setUp(self):
        self.old = redisql.GracefulRedisServer
        redisql.GracefulRedisServer = FakeRedis
        self.storage = WeaveStorage.get('redisql',
                                        sqluri='sqlite:///:memory:')
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
        self.assertEquals(self.storage._conn.set_called, 1)
        self.assertEquals(self.storage._conn.keys(), ['meta:global:1'])

        # this should remove the cache
        self.storage.delete_item(_UID, 'meta', 'global')
        self.assertEquals(self.storage._conn.keys(), [])

        items = [{'id': 'global', 'payload': 'xxx'},
                 {'id': 'other', 'payload': 'xxx'},
                ]
        self.storage.set_items(_UID, 'meta', items)
        self.assertEquals(self.storage._conn.keys(), ['meta:global:1'])

        # this should remove the cache
        self.storage.delete_items(_UID, 'meta')
        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 0)
        self.assertEquals(self.storage._conn.keys(), [])


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestRediSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
