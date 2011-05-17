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
import time
from decimal import Decimal

try:
    from syncstorage.storage.memcachedsql import MemcachedSQLStorage
    MEMCACHED = True
except ImportError:
    MEMCACHED = False
from syncstorage.storage import SyncStorage
from services.util import BackendError

_UID = 1
_PLD = '*' * 500

# manual registration
if MEMCACHED:
    SyncStorage.register(MemcachedSQLStorage)

    class TestMemcachedSQLStorage(unittest.TestCase):

        def setUp(self):
            kw = {'sqluri': 'sqlite:///:memory:',
                  'use_quota': True,
                  'quota_size': 5120,
                  'create_tables': True}

            self.storage = SyncStorage.get('memcached', **kw)

            # make sure we have the standard collections in place
            for name in ('client', 'crypto', 'forms', 'history'):
                self.storage.set_collection(_UID, name)

        def tearDown(self):
            self.storage.cache.flush_all()
            self.storage.delete_user(_UID)

        def _is_up(self):
            try:
                self.storage.cache.set('test', 1)
            except BackendError:
                return False
            return self.storage.cache.get('test') == 1

        def test_basic(self):
            if not self._is_up():
                return
            # just make sure calls goes through
            self.storage.set_user(_UID, email='tarek@ziade.org')
            self.storage.set_collection(_UID, 'col1')
            self.storage.set_item(_UID, 'col1', '1', payload=_PLD)

            # these calls should be cached
            res = self.storage.get_item(_UID, 'col1', '1')
            self.assertEquals(res['payload'], _PLD)

            # this should remove the cache
            self.storage.delete_items(_UID, 'col1')
            items = self.storage.get_items(_UID, 'col1')
            self.assertEquals(len(items), 0)

        def test_meta_global(self):
            if not self._is_up():
                return
            self.storage.set_user(_UID, email='tarek@ziade.org')
            self.storage.set_collection(_UID, 'meta')
            self.storage.set_item(_UID, 'meta', 'global', payload=_PLD)

            # these calls should be cached
            res = self.storage.get_item(_UID, 'meta', 'global')
            self.assertEquals(res['payload'], _PLD)

            # we should find in the cache these items:
            #   - the "global" wbo for the "meta" collection
            #   - the size of all wbos
            if self._is_up():
                meta = self.storage.cache.get('1:meta:global')
                self.assertEquals(meta['id'], 'global')
                size = self.storage.cache.get('1:size')
                self.assertEquals(size, 500)

            # this should remove the cache for meta global
            self.storage.delete_item(_UID, 'meta', 'global')

            if self._is_up():
                meta = self.storage.cache.get('1:meta:global')
                self.assertEquals(meta, None)
                size = self.storage.cache.get('1:size')
                self.assertEquals(size, None)

            # let's store some items in the meta collection
            # and checks that the global object is uploaded
            items = [{'id': 'global', 'payload': 'xyx'},
                    {'id': 'other', 'payload': 'xxx'},
                    ]
            self.storage.set_items(_UID, 'meta', items)

            if self._is_up():
                global_ = self.storage.cache.get('1:meta:global')
                self.assertEquals(global_['payload'], 'xyx')

            # this should remove the cache
            self.storage.delete_items(_UID, 'meta')
            items = self.storage.get_items(_UID, 'col')
            self.assertEquals(len(items), 0)

            if self._is_up():
                meta = self.storage.cache.get('1:meta:global')
                self.assertEquals(meta, None)

        def test_tabs(self):
            if not self._is_up():  # no memcached == no tabs
                return

            self.storage.set_user(_UID, email='tarek@ziade.org')
            self.storage.set_collection(_UID, 'tabs')
            self.storage.set_item(_UID, 'tabs', '1', payload=_PLD)

            # these calls should be cached
            res = self.storage.get_item(_UID, 'tabs', '1')
            self.assertEquals(res['payload'], _PLD)
            tabs = self.storage.cache.get('1:tabs')
            self.assertEquals(tabs['1']['payload'], _PLD)

            # this should remove the cache
            self.storage.delete_item(_UID, 'tabs', '1')
            tabs = self.storage.cache.get('1:tabs')
            self.assertFalse('1' in tabs)

            #  adding some stuff
            items = [{'id': '1', 'payload': 'xxx'},
                    {'id': '2', 'payload': 'xxx'}]
            self.storage.set_items(_UID, 'tabs', items)
            tabs = self.storage.cache.get('1:tabs')
            self.assertEquals(len(tabs), 2)

            # this should remove the cache
            self.storage.delete_items(_UID, 'tabs')
            items = self.storage.get_items(_UID, 'tabs')
            self.assertEquals(len(items), 0)
            tabs = self.storage.cache.get('1:tabs')
            self.assertEquals(tabs, {})

        def test_size(self):
            # make sure we get the right size
            if not self._is_up():  # no memcached == no size
                return
            self.storage.set_user(_UID, email='tarek@ziade.org')
            self.storage.set_collection(_UID, 'tabs')
            self.storage.set_collection(_UID, 'foo')
            self.storage.set_item(_UID, 'foo', '1', payload=_PLD)
            self.storage.set_item(_UID, 'tabs', '1', payload=_PLD)
            wanted = len(_PLD) * 2 / 1024.
            self.assertEquals(self.storage.get_total_size(_UID), wanted)

        def test_collection_stamps(self):
            if not self._is_up():
                return

            self.storage.set_user(_UID, email='tarek@ziade.org')
            self.storage.set_collection(_UID, 'tabs')
            self.storage.set_collection(_UID, 'foo')
            self.storage.set_collection(_UID, 'baz')

            self.storage.set_item(_UID, 'tabs', '1', payload=_PLD * 200)
            self.storage.set_item(_UID, 'foo', '1', payload=_PLD * 200)

            stamps = self.storage.get_collection_timestamps(_UID)  # pump cache
            if self._is_up():
                tabstamps = self.storage.cache.get('1:collections:stamp:tabs')
                self.assertEquals(stamps['tabs'], tabstamps)

            stamps2 = self.storage.get_collection_timestamps(_UID)
            self.assertEquals(len(stamps), len(stamps2))
            if self._is_up():
                self.assertEquals(len(stamps), 2)
            else:
                self.assertEquals(len(stamps), 1)

            # checking the stamps
            if self._is_up():
                stamps = self.storage.cache.get('1:stamps')
                self.assertEquals(stamps.keys(), ['foo'])

            # adding a new item should invalidate the stamps cache
            self.storage.set_item(_UID, 'baz', '2', payload=_PLD * 200)

            # checking the stamps
            if self._is_up():
                stamps = self.storage.cache.get('1:stamps')
                self.assertEqual(stamps, None)

            stamps = self.storage.get_collection_timestamps(_UID)
            if self._is_up():
                _stamps = self.storage.cache.get('1:stamps')
                keys = _stamps.keys()
                keys.sort()
                self.assertEquals(keys, ['baz', 'foo'])

            # deleting the item should also update the stamp
            time.sleep(0.2)    # to make sure the stamps differ
            self.storage.delete_item(_UID, 'baz', '2')
            stamps = self.storage.get_collection_timestamps(_UID)
            self.assertFalse('baz' in stamps)

        def test_collection_sizes(self):
            if not self._is_up():  # no memcached
                return
            kw = {'sqluri': 'sqlite:///:memory:',
                  'use_quota': True,
                  'quota_size': 5120,
                  'create_tables': True}

            storage = SyncStorage.get('memcached', **kw)

            # setting the tabs in memcache
            tabs = {'mCwylprUEiP5':
                     {'payload': '*' * 500,
                      'id': 'mCwylprUEiP5',
                      'modified': Decimal('1299142695.76')}}
            storage.cache.set_tabs(1, tabs)

            size = storage.get_collection_sizes(1)
            self.assertEqual(size['tabs'], 500)


def test_suite():
    suite = unittest.TestSuite()
    if MEMCACHED:
        suite.addTest(unittest.makeSuite(TestMemcachedSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
