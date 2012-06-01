# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import unittest2
import time
from tempfile import mkstemp
import os

try:
    from syncstorage.storage.memcachedsql import MemcachedSQLStorage  # NOQA
    from syncstorage.storage.memcachedsql import QUOTA_RECALCULATION_PERIOD
    from syncstorage.storage.memcachedsql import QUOTA_RECALCULATION_THRESHOLD
    MEMCACHED = True
except ImportError:
    MEMCACHED = False

from mozsvc.exceptions import BackendError
from mozsvc.plugin import load_from_settings

from syncstorage.util import get_timestamp
from syncstorage.tests.support import StorageTestCase

_UID = 1
_PLD = '*' * 500


class TestMemcachedSQLStorage(StorageTestCase):

    def setUp(self):
        super(TestMemcachedSQLStorage, self).setUp()
        if not MEMCACHED:
            raise unittest2.SkipTest

        fd, self.dbfile = mkstemp()
        os.close(fd)

        self.fn = 'syncstorage.storage.memcachedsql.MemcachedSQLStorage'

        settings = {'storage.backend': self.fn,
                    'storage.sqluri': 'sqlite:///%s' % self.dbfile,
                    'storage.use_quota': True,
                    'storage.quota_size': 5242880,
                    'storage.create_tables': True}

        self.storage = load_from_settings("storage", settings)

        # Check that memcached is actually running.
        try:
            self.storage.cache.set('test', 1)
            assert self.storage.cache.get('test') == 1
        except BackendError:
            raise unittest2.SkipTest

        # make sure we have the standard collections in place
        for name in ('client', 'crypto', 'forms', 'history'):
            self.storage.set_items(_UID, name, [])

    def tearDown(self):
        self.storage.cache.flush_all()
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)

    def test_basic(self):
        # just make sure calls goes through
        self.storage.set_item(_UID, 'col1', '1', payload=_PLD)

        # these calls should be cached
        res = self.storage.get_item(_UID, 'col1', '1')
        self.assertEquals(res['payload'], _PLD)

        # this should remove the cache
        self.storage.delete_items(_UID, 'col1')
        self.assertEquals(self.storage.get_items(_UID, 'col1'), None)

    def test_meta_global(self):
        self.storage.set_item(_UID, 'meta', 'global', payload=_PLD)

        # these calls should be cached
        res = self.storage.get_item(_UID, 'meta', 'global')
        self.assertEquals(res['payload'], _PLD)

        # we should find in the cache these items:
        #   - the "global" bso for the "meta" collection
        #   - the size of all bsos
        meta = self.storage.cache.get('1:meta:global')
        self.assertEquals(meta['id'], 'global')
        size = self.storage.cache.get('1:size')
        self.assertEquals(size, len(_PLD))

        # this should remove the cache for meta global
        self.storage.delete_item(_UID, 'meta', 'global')

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

        global_ = self.storage.cache.get('1:meta:global')
        self.assertEquals(global_['payload'], 'xyx')

        # this should remove the cache
        self.storage.delete_items(_UID, 'meta')
        self.assertEquals(self.storage.get_items(_UID, 'col'), None)

        meta = self.storage.cache.get('1:meta:global')
        self.assertEquals(meta, None)

    def test_tabs(self):
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
        # storing 2 BSOs
        self.storage.set_item(_UID, 'foo', '1', payload=_PLD)
        self.storage.set_item(_UID, 'tabs', '1', payload=_PLD)

        wanted = len(_PLD) * 2
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

        # removing the size in memcache to check that we
        # get back the right value
        self.storage.cache.delete('%d:size' % _UID)
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

        # adding an item should increment the cached size.
        self.storage.set_item(_UID, 'foo', '2', payload=_PLD)
        wanted += len(_PLD)
        self.assertEquals(self.storage.get_total_size(_UID), wanted)

        # if we suffer a cache clear, then get_size_left should not
        # fall back to the database, while get_total_size should.
        quota_size = self.storage.quota_size
        self.storage.cache.delete('%d:size' % _UID)
        self.assertEquals(self.storage.get_size_left(_UID), quota_size)
        self.assertEquals(self.storage.get_total_size(_UID), wanted)
        # that should have re-populated the cache.
        self.assertEquals(self.storage.get_size_left(_UID),
                          quota_size - wanted)

    def test_collection_stamps(self):
        self.storage.set_item(_UID, 'tabs', '1', payload=_PLD * 200)
        self.storage.set_item(_UID, 'foo', '1', payload=_PLD * 200)

        stamps = self.storage.get_collection_timestamps(_UID)  # pump cache
        cached_stamps = self.storage.cache.get('1:stamps')
        self.assertEquals(stamps['tabs'], cached_stamps['tabs'])

        stamps2 = self.storage.get_collection_timestamps(_UID)
        self.assertEquals(len(stamps), len(stamps2))
        self.assertEquals(len(stamps), 2)

        # checking the stamps
        stamps = self.storage.cache.get('1:stamps')
        keys = stamps.keys()
        keys.sort()
        self.assertEquals(keys, ['foo', 'tabs'])

        # adding a new item should modify the stamps cache
        now = get_timestamp()
        self.storage.set_item(_UID, 'baz', '2', payload=_PLD * 200,
                              storage_time=now)

        # checking the stamps
        stamps = self.storage.cache.get('1:stamps')
        self.assertEqual(stamps['baz'], now)

        stamps = self.storage.get_collection_timestamps(_UID)
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
        self.assertEqual(self.storage.cache.get('1:size'),
                         sum(size.values()))

    def test_collection_sizes(self):
        # setting the tabs in memcache
        tabs = {'mCwylprUEiP5':
                {'payload': '*' * 100,
                'id': 'mCwylprUEiP5',
                'modified': 1299142695760}}
        self.storage.cache.set_tabs(1, tabs)
        size = self.storage.get_collection_sizes(1)
        self.assertEqual(size['tabs'], 100)

    def test_flush_all(self):
        # just make sure calls goes through
        self.storage.set_item(_UID, 'col1', '1', payload=_PLD)

        # these calls should be cached
        res = self.storage.get_item(_UID, 'col1', '1')
        self.assertEquals(res['payload'], _PLD)

        # this should remove the cache
        self.storage.delete_items(_UID, 'col1')
        self.assertEquals(self.storage.get_items(_UID, 'col1'), None)

        self.storage.set_item(_UID, 'col1', '1', payload=_PLD)
        self.storage.set_item(_UID, 'col1', '2', payload=_PLD)
        self.storage.set_item(_UID, 'col1', '3', payload=_PLD)
        self.storage.set_item(_UID, 'col2', '4', payload=_PLD)

        items = self.storage.get_items(_UID, 'col1')
        self.assertEquals(len(items), 3)

        self.storage.delete_storage(_UID)
        self.assertEquals(self.storage.get_items(_UID, 'col1'), None)

        stamps = self.storage.get_collection_timestamps(_UID)
        self.assertEquals(len(stamps), 0)

    def test_get_timestamp_of_empty_collection(self):
        # This tests for the error behind Bug 693893.
        # Max timestamp for an empty collection should be None.
        ts = self.storage.get_collection_timestamp(_UID, "meta")
        self.assertEquals(ts, None)

    def test_recalculation_of_cached_quota_usage(self):
        storage = self.storage
        sqlstorage = self.storage.sqlstorage

        # Create a large BSO, to ensure that it's close to quota size.
        payload_size = storage.quota_size - QUOTA_RECALCULATION_THRESHOLD + 1
        payload = "X" * payload_size

        # After writing it, size in memcached and sql should be the same.
        storage.set_item(_UID, 'foo', '1', payload=payload)
        self.assertEquals(storage.get_total_size(_UID), payload_size)
        self.assertEquals(sqlstorage.get_total_size(_UID), payload_size)

        # Deleting the BSO in the database won't adjust the cached size.
        sqlstorage.delete_item(_UID, 'foo', '1')
        self.assertEquals(storage.get_total_size(_UID), payload_size)
        self.assertEquals(sqlstorage.get_total_size(_UID), 0)

        # Adjust the cache to pretend that hasn't been recalculated lately.
        last_recalc = storage.cache.get("1:size:ts")
        last_recalc -= QUOTA_RECALCULATION_PERIOD + 1
        storage.cache.set("1:size:ts", last_recalc)

        # Now it should recalculate when asked for the size.
        self.assertEquals(storage.get_total_size(_UID), 0)
        self.assertEquals(sqlstorage.get_total_size(_UID), 0)


def test_suite():
    suite = unittest2.TestSuite()
    if MEMCACHED:
        suite.addTest(unittest2.makeSuite(TestMemcachedSQLStorage))
    return suite

if __name__ == "__main__":
    unittest2.main(defaultTest="test_suite")
