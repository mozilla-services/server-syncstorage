# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import unittest2
import time

try:
    from syncstorage.storage.memcached import MemcachedStorage  # NOQA
    from syncstorage.storage.memcached import SIZE_RECALCULATION_PERIOD
    MEMCACHED = True
except ImportError:
    MEMCACHED = False

from mozsvc.exceptions import BackendError

from syncstorage.tests.support import StorageTestCase
from syncstorage.tests.test_storage import StorageTestsMixin

from syncstorage.storage import (load_storage_from_settings,
                                 CollectionNotFoundError,
                                 ItemNotFoundError)

_UID = 1
_PLD = '*' * 500


class TestMemcachedSQLStorage(StorageTestCase, StorageTestsMixin):

    TEST_INI_FILE = "tests-memcached.ini"

    def setUp(self):
        super(TestMemcachedSQLStorage, self).setUp()
        if not MEMCACHED:
            raise unittest2.SkipTest

        settings = self.config.registry.settings
        self.storage = load_storage_from_settings("storage", settings)

        # Check that memcached is actually running.
        try:
            self.storage.cache.set('test', 1)
            assert self.storage.cache.get('test') == 1
        except BackendError:
            raise unittest2.SkipTest

    def test_basic(self):
        # just make sure calls goes through
        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})

        # these calls should be cached
        res = self.storage.get_item(_UID, 'col1', '1')
        self.assertEquals(res['payload'], _PLD)

        # this should remove the cache
        self.storage.delete_collection(_UID, 'col1')
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'col1')

    def test_meta_global(self):
        self.storage.set_item(_UID, 'meta', 'global', {'payload': _PLD})
        sqlstorage = self.storage.storage

        # This call should be cached.
        res = self.storage.get_item(_UID, 'meta', 'global')
        self.assertEquals(res['payload'], _PLD)

        # That should have populated some cache entries.
        collection = self.storage.cache.get('1:c:meta')
        self.assertEquals(collection["items"].keys(), ["global"])
        metadata = self.storage.cache.get('1:metadata')
        self.assertTrue(metadata['collections']['meta'])
        self.assertEquals(metadata['size'], len(_PLD))

        # It should have also written it through to the underlying store.
        item = sqlstorage.get_item(_UID, 'meta', 'global')
        self.assertEquals(item['payload'], _PLD)

        # This should remove the cache entry for meta global
        self.storage.delete_item(_UID, 'meta', 'global')

        collection = self.storage.cache.get('1:c:meta')
        self.assertEquals(collection["items"].keys(), [])
        metadata = self.storage.cache.get('1:metadata')
        self.assertEquals(metadata['size'], len(_PLD))

        # It should have also remove it from the underlying store.
        self.assertRaises(ItemNotFoundError,
                          sqlstorage.get_item, _UID, 'meta', 'global')

        # let's store some items in the meta collection
        # and checks that the global object is uploaded
        items = [{'id': 'global', 'payload': 'xyx'},
                 {'id': 'other', 'payload': 'xxx'}]
        self.storage.set_items(_UID, 'meta', items)

        collection = self.storage.cache.get('1:c:meta')
        self.assertEquals(sorted(collection["items"].keys()),
                          ['global', 'other'])

        # this should remove the cache
        self.storage.delete_collection(_UID, 'meta')
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'meta')
        self.assertRaises(CollectionNotFoundError,
                          sqlstorage.get_items, _UID, 'meta')

        collection = self.storage.cache.get('1:c:meta')
        self.assertEquals(collection, None)

    def test_tabs(self):
        self.storage.set_item(_UID, 'tabs', '1', {'payload': _PLD})
        sqlstorage = self.storage.storage

        # these calls should be cached
        res = self.storage.get_item(_UID, 'tabs', '1')
        self.assertEquals(res['payload'], _PLD)
        collection = self.storage.cache.get('1:c:tabs')
        self.assertEquals(collection['items']['1']['payload'], _PLD)

        # it should not exist in the underlying store
        self.assertRaises(CollectionNotFoundError,
                          sqlstorage.get_item, _UID, 'tabs', '1')

        # this should remove the cache
        self.storage.delete_item(_UID, 'tabs', '1')
        collection = self.storage.cache.get('1:c:tabs')
        self.assertEquals(collection['items'].keys(), [])

        #  adding some stuff
        items = [{'id': '1', 'payload': 'xxx'},
                {'id': '2', 'payload': 'xxx'}]
        self.storage.set_items(_UID, 'tabs', items)
        collection = self.storage.cache.get('1:c:tabs')
        self.assertEquals(len(collection['items']), 2)

        # this should remove the cache
        self.storage.delete_collection(_UID, 'tabs')
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'tabs')
        collection = self.storage.cache.get('1:c:tabs')
        self.assertEquals(collection, None)

    def test_size(self):
        # storing 2 BSOs
        self.storage.set_item(_UID, 'foo', '1', {'payload': _PLD})
        self.storage.set_item(_UID, 'tabs', '1', {'payload': _PLD})

        wanted = len(_PLD) * 2
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

        # adding an item should increment the cached size.
        self.storage.set_item(_UID, 'foo', '2', {'payload': _PLD})
        wanted += len(_PLD)
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

        # if we suffer a cache clear, the size will revert to zero.
        self.storage.cache.delete('%d:metadata' % _UID)
        self.assertEquals(self.storage.get_total_size(_UID), 0)

        # unless we explicitly ask it to recalculate.
        self.assertEquals(self.storage.get_total_size(_UID, True), wanted)
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

    def test_collection_versions(self):
        self.storage.delete_storage(_UID)
        self.storage.set_item(_UID, 'tabs', '1', {'payload': _PLD * 200})
        self.storage.set_item(_UID, 'foo', '1', {'payload': _PLD * 200})

        versions = self.storage.get_collection_versions(_UID)
        cached_versions = self.storage.cache.get('1:metadata')['collections']
        self.assertEquals(versions['tabs'], cached_versions['tabs'])

        versions2 = self.storage.get_collection_versions(_UID)
        self.assertEquals(len(versions), len(versions2))
        self.assertEquals(len(versions), 2)

        # checking the versions
        versions = self.storage.cache.get('1:metadata')['collections']
        keys = versions.keys()
        keys.sort()
        self.assertEquals(keys, ['foo', 'tabs'])

        # adding a new item should modify the versions cache
        res = self.storage.set_item(_UID, 'baz', '2', {'payload': _PLD * 200})
        ver = res["version"]

        # checking the versions
        versions = self.storage.cache.get('1:metadata')['collections']
        self.assertEqual(versions['baz'], ver)
        keys = versions.keys()
        keys.sort()
        self.assertEquals(keys, ['baz', 'foo', 'tabs'])

        # deleting the item should also update the version
        cached_size = self.storage.cache.get('1:metadata')['size']
        ver = self.storage.delete_item(_UID, 'baz', '2')
        versions = self.storage.get_collection_versions(_UID)
        self.assertEqual(versions['baz'], ver)

        # that should have left the cached size alone.
        self.assertEquals(self.storage.cache.get('1:metadata')['size'],
                          cached_size)

        # until we recalculate it by reading out the sizes.
        sizes = self.storage.get_collection_sizes(_UID)
        self.assertEqual(self.storage.cache.get('1:metadata')['size'],
                         sum(sizes.values()))

    def test_collection_sizes(self):
        # setting the tabs in memcache
        tabs = {'version': 1299142695760,
                'items': {'mCwylprUEiP5':
                  {'payload': '*' * 100,
                   'id': 'mCwylprUEiP5',
                   'version': 1299142695760}}}
        self.storage.cache.set('1:c:tabs', tabs)
        size = self.storage.get_collection_sizes(1)
        self.assertEqual(size['tabs'], 100)

    def test_that_cache_is_cleared_when_things_are_deleted(self):
        # just make sure calls goes through
        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})

        # these calls should be cached
        res = self.storage.get_item(_UID, 'col1', '1')
        self.assertEquals(res['payload'], _PLD)

        # this should remove the cache
        self.storage.delete_collection(_UID, 'col1')
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'col1')

        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})
        self.storage.set_item(_UID, 'col1', '2', {'payload': _PLD})
        self.storage.set_item(_UID, 'col1', '3', {'payload': _PLD})
        self.storage.set_item(_UID, 'col2', '4', {'payload': _PLD})

        items = self.storage.get_items(_UID, 'col1')["items"]
        self.assertEquals(len(items), 3)

        self.storage.delete_storage(_UID)
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'col1')

        versions = self.storage.get_collection_versions(_UID)
        self.assertEquals(len(versions), 0)

    def test_get_version_of_empty_collection(self):
        # This tests for derivative of the error behind Bug 693893.
        # Getting version for a non-existent collection should raise error.
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_collection_version, _UID, "meta")

    def test_recalculation_of_cached_quota_usage(self):
        storage = self.storage
        sqlstorage = self.storage.storage

        # After writing, size in memcached and sql should be the same.
        storage.set_item(_UID, 'foo', '1', {'payload': _PLD})
        self.assertEquals(storage.get_total_size(_UID), len(_PLD))
        self.assertEquals(storage.get_total_size(_UID, True), len(_PLD))
        self.assertEquals(sqlstorage.get_total_size(_UID), len(_PLD))

        # Deleting the BSO in the database won't adjust the cached size.
        # It will refuse to recalculate again so soon.
        sqlstorage.delete_item(_UID, 'foo', '1')
        self.assertEquals(sqlstorage.get_total_size(_UID), 0)
        self.assertEquals(storage.get_total_size(_UID), len(_PLD))
        self.assertEquals(storage.get_total_size(_UID, True), len(_PLD))

        # Adjust the cache to pretend that hasn't been recalculated lately.
        metadata = storage.cache.get('1:metadata')
        last_recalc = metadata['last_size_recalc']
        last_recalc -= SIZE_RECALCULATION_PERIOD + 1
        metadata['last_size_recalc'] = last_recalc
        storage.cache.set("1:metadata", metadata)

        # Now it should recalculate when asked to do so.
        self.assertEquals(sqlstorage.get_total_size(_UID), 0)
        self.assertEquals(storage.get_total_size(_UID), len(_PLD))
        self.assertEquals(storage.get_total_size(_UID, True), 0)


def test_suite():
    suite = unittest2.TestSuite()
    if MEMCACHED:
        suite.addTest(unittest2.makeSuite(TestMemcachedSQLStorage))
    return suite

if __name__ == "__main__":
    unittest2.main(defaultTest="test_suite")
