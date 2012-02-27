# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest
import time
from tempfile import mkstemp
import os

try:
    from syncstorage.storage.memcachedsql import MemcachedSQLStorage # NOQA
    MEMCACHED = True
except ImportError:
    MEMCACHED = False

from mozsvc.exceptions import BackendError
from mozsvc.plugin import load_from_settings

from syncstorage.util import get_timestamp

_UID = 1
_PLD = '*' * 500

# manual registration
if MEMCACHED:

    class TestMemcachedSQLStorage(unittest.TestCase):

        def setUp(self):
            fd, self.dbfile = mkstemp()
            os.close(fd)

            self.fn = 'syncstorage.storage.memcachedsql.MemcachedSQLStorage'

            settings = {'storage.backend': self.fn,
                        'storage.sqluri': 'sqlite:///%s' % self.dbfile,
                        'storage.use_quota': True,
                        'storage.quota_size': 5120,
                        'storage.create_tables': True}

            self.storage = load_from_settings("storage", settings)

            # make sure we have the standard collections in place

            for name in ('client', 'crypto', 'forms', 'history'):
                self.storage.set_items(_UID, name, [])

        def tearDown(self):
            self.storage.cache.flush_all()
            if os.path.exists(self.dbfile):
                os.remove(self.dbfile)

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
            self.storage.set_item(_UID, 'meta', 'global', payload=_PLD)

            # these calls should be cached
            res = self.storage.get_item(_UID, 'meta', 'global')
            self.assertEquals(res['payload'], _PLD)

            # we should find in the cache these items:
            #   - the "global" bso for the "meta" collection
            #   - the size of all bsos
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

            # storing 2 BSOs
            self.storage.set_item(_UID, 'foo', '1', payload=_PLD)
            self.storage.set_item(_UID, 'tabs', '1', payload=_PLD)

            # value in KB (around 1K)
            wanted = len(_PLD) * 2 / 1024.
            self.assertEquals(self.storage.get_total_size(_UID), wanted)

            # removing the size in memcache to check that we
            # get back the right value
            self.storage.cache.delete('%d:size' % _UID)
            self.assertEquals(self.storage.get_total_size(_UID), wanted)

        def test_collection_stamps(self):
            if not self._is_up():
                return

            self.storage.set_item(_UID, 'tabs', '1', payload=_PLD * 200)
            self.storage.set_item(_UID, 'foo', '1', payload=_PLD * 200)

            stamps = self.storage.get_collection_timestamps(_UID)  # pump cache
            if self._is_up():
                cached_stamps = self.storage.cache.get('1:stamps')
                self.assertEquals(stamps['tabs'], cached_stamps['tabs'])

            stamps2 = self.storage.get_collection_timestamps(_UID)
            self.assertEquals(len(stamps), len(stamps2))
            if self._is_up():
                self.assertEquals(len(stamps), 2)
            else:
                self.assertEquals(len(stamps), 1)

            # checking the stamps
            if self._is_up():
                stamps = self.storage.cache.get('1:stamps')
                keys = stamps.keys()
                keys.sort()
                self.assertEquals(keys, ['foo', 'tabs'])

            # adding a new item should modify the stamps cache
            now = get_timestamp()
            self.storage.set_item(_UID, 'baz', '2', payload=_PLD * 200,
                                  storage_time=now)

            # checking the stamps
            if self._is_up():
                stamps = self.storage.cache.get('1:stamps')
                self.assertEqual(stamps['baz'], now)

            stamps = self.storage.get_collection_timestamps(_UID)
            if self._is_up():
                _stamps = self.storage.cache.get('1:stamps')
                keys = _stamps.keys()
                keys.sort()
                self.assertEquals(keys, ['baz', 'foo', 'tabs'])

            # deleting the item should also update the stamp
            time.sleep(0.2)    # to make sure the stamps differ
            now = get_timestamp()
            self.storage.delete_item(_UID, 'baz', '2', storage_time=now)
            stamps = self.storage.get_collection_timestamps(_UID)
            self.assertEqual(stamps['baz'], now)

            # and that kills the size cache
            self.assertTrue(self.storage.cache.get('1:size') is None)

            # until we asked for it again
            size = self.storage.get_collection_sizes(1)
            self.assertEqual(self.storage.cache.get('1:size') / 1024,
                             sum(size.values()))

        def test_collection_sizes(self):
            if not self._is_up():  # no memcached
                return
            # setting the tabs in memcache
            tabs = {'mCwylprUEiP5':
                    {'payload': '*' * 1024,
                    'id': 'mCwylprUEiP5',
                    'modified': 1299142695760}}
            self.storage.cache.set_tabs(1, tabs)
            size = self.storage.get_collection_sizes(1)
            self.assertEqual(size['tabs'], 1.)

        def test_flush_all(self):
            if not self._is_up():
                return
            # just make sure calls goes through
            self.storage.set_item(_UID, 'col1', '1', payload=_PLD)

            # these calls should be cached
            res = self.storage.get_item(_UID, 'col1', '1')
            self.assertEquals(res['payload'], _PLD)

            # this should remove the cache
            self.storage.delete_items(_UID, 'col1')
            items = self.storage.get_items(_UID, 'col1')
            self.assertEquals(len(items), 0)

            self.storage.set_item(_UID, 'col1', '1', payload=_PLD)
            self.storage.set_item(_UID, 'col1', '2', payload=_PLD)
            self.storage.set_item(_UID, 'col1', '3', payload=_PLD)
            self.storage.set_item(_UID, 'col2', '4', payload=_PLD)

            items = self.storage.get_items(_UID, 'col1')
            self.assertEquals(len(items), 3)

            self.storage.delete_storage(_UID)
            items = self.storage.get_items(_UID, 'col1')
            self.assertEquals(len(items), 0)

            stamps = self.storage.get_collection_timestamps(_UID)
            self.assertEquals(len(stamps), 0)


def test_suite():
    suite = unittest.TestSuite()
    if MEMCACHED:
        suite.addTest(unittest.makeSuite(TestMemcachedSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
