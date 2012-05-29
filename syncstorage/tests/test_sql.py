# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import time
import unittest2

from mozsvc.plugin import load_and_register
from mozsvc.tests.support import get_test_configurator

from syncstorage.storage.sqlmappers import get_bso_table_name
from syncstorage.storage import SyncStorage
from syncstorage.storage.sql import SQLStorage
from syncstorage.tests.support import StorageTestCase

from mozsvc.exceptions import BackendError

_UID = 1
_PLD = '*' * 500


SyncStorage.register(SQLStorage)


class TestSQLStorage(StorageTestCase):

    def setUp(self):
        super(TestSQLStorage, self).setUp()

        self.storage = load_and_register("storage", self.config)

        # make sure we have the standard collections in place
        for name in ('client', 'crypto', 'forms', 'history', 'key', 'meta',
                     'bookmarks', 'prefs', 'tabs', 'passwords'):
            self.storage.set_items(_UID, name, [])

        self._cfiles = []

    def tearDown(self):
        for file_ in self._cfiles:
            if os.path.exists(file_):
                os.remove(file_)
        super(TestSQLStorage, self).tearDown()

    def _add_cleanup(self, path):
        self._cfiles.append(path)

    def test_items(self):
        self.assertFalse(self.storage.item_exists(_UID, 'col', 1))
        self.assertEquals(self.storage.get_items(_UID, 'col'), None)

        self.storage.set_item(_UID, 'col', 1, payload=_PLD)
        res = self.storage.get_item(_UID, 'col', 1)
        self.assertEquals(res['payload'], _PLD)

        self.storage.set_item(_UID, 'col', 2, payload=_PLD)

        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 2)

        self.storage.delete_item(_UID, 'col', 1)
        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 1)

        self.storage.delete_items(_UID, 'col')
        self.assertEquals(self.storage.get_items(_UID, 'col'), None)

        self.storage.set_items(_UID, 'col',
                               items=[{'id': 'o', 'payload': _PLD}])
        res = self.storage.get_item(_UID, 'col', 'o')
        self.assertEquals(res['payload'], _PLD)

    def test_get_collection_timestamps(self):
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col2', 1, payload=_PLD)

        timestamps = self.storage.get_collection_timestamps(_UID)
        names = timestamps.keys()
        self.assertTrue('col1' in names)
        self.assertTrue('col2' in names)
        col1 = self.storage.get_collection_timestamp(_UID, 'col2')
        self.assertAlmostEquals(col1, timestamps['col2'])

        # check that when we have several users, the method
        # still returns the same timestamps for the first user
        # which differs from the second user
        time.sleep(1.)
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col2', 1, payload=_PLD)

        user1_timestamps = self.storage.get_collection_timestamps(_UID)
        user1_timestamps = user1_timestamps.items()
        user1_timestamps.sort()

        user2_timestamps = self.storage.get_collection_timestamps(2)
        user2_timestamps = user2_timestamps.items()
        user2_timestamps.sort()

        self.assertNotEqual(user1_timestamps, user2_timestamps)

    def test_storage_size(self):
        before = self.storage.get_total_size(_UID)
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col1', 2, payload=_PLD)
        wanted = len(_PLD) * 2
        self.assertEquals(self.storage.get_total_size(_UID) - before, wanted)

    def test_ttl(self):
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col1', 2, payload=_PLD, ttl=0)
        time.sleep(1.1)
        self.assertEquals(len(self.storage.get_items(_UID, 'col1')), 1)
        self.assertEquals(len(self.storage.get_items(_UID, 'col1',
                                                filters={'ttl': ('>', -1)})),
                                                2)

    def test_dashed_ids(self):
        id1 = '{ec1b7457-003a-45a9-bf1c-c34e37225ad7}'
        id2 = '{339f52e1-deed-497c-837a-1ab25a655e37}'
        self.storage.set_item(_UID, 'col1', id1, payload=_PLD)
        self.storage.set_item(_UID, 'col1', id2, payload=_PLD * 89)
        self.assertEquals(len(self.storage.get_items(_UID, 'col1')), 2)

        # now trying to delete them
        self.storage.delete_items(_UID, 'col1', item_ids=[id1, id2])
        # XXX TODO: technically this should return an empty list
        # rather than None, but the SQLStorage backend doesn't properly
        # track the last-modified time so it can't do it right just yet.
        self.assertFalse(self.storage.get_items(_UID, 'col1'))
        #self.assertEquals(len(self.storage.get_items(_UID, 'col1')), 0)

    def test_no_create(self):
        # testing the create_tables option
        config = get_test_configurator(__file__, 'tests3.ini')
        storage = load_and_register("storage", config)

        bsos = [{"id": "TEST", "payload": _PLD}]
        # this should fail because the table is absent
        self.assertRaises(BackendError,
                          storage.set_items, _UID, "test", bsos)

        # create_table = false
        config = get_test_configurator(__file__, 'tests4.ini')
        storage = load_and_register("storage", config)
        sqlfile = storage.sqluri.split('sqlite:///')[-1]
        try:
            # this should fail because the table is absent
            self.assertRaises(BackendError,
                              storage.set_items, _UID, "test", bsos)
        finally:
            # removing the db created
            if os.path.exists(sqlfile):
                os.remove(sqlfile)

        # create_table = true
        config = get_test_configurator(__file__, 'tests2.ini')
        storage = load_and_register("storage", config)

        # this should work because the table is no longer absent
        storage.set_items(_UID, "test", [])

    def test_shard(self):
        self._add_cleanup(os.path.join('/tmp', 'tests2.db'))

        # make shure we do shard
        config = get_test_configurator(__file__, 'tests2.ini')
        storage = load_and_register("storage", config)

        res = storage._engine.execute('select count(*) from bso1')
        self.assertEqual(res.fetchall()[0][0], 0)

        # doing a few things on the DB
        id1 = '{ec1b7457-003a-45a9-bf1c-c34e37225ad7}'
        id2 = '{339f52e1-deed-497c-837a-1ab25a655e37}'
        storage.set_item(_UID, 'col1', id1, payload=_PLD)
        storage.set_item(_UID, 'col1', id2, payload=_PLD * 89)
        self.assertEquals(len(storage.get_items(_UID, 'col1')), 2)

        # now making sure we did that in the right table
        table = get_bso_table_name(_UID)
        self.assertEqual(table, 'bso1')
        res = storage._engine.execute('select count(*) from bso1')
        self.assertEqual(res.fetchall()[0][0], 2)

    def test_nopool(self):
        # make sure the pool is forced to NullPool when sqlite is used.
        config = get_test_configurator(__file__, 'tests2.ini')
        storage = load_and_register("storage", config)
        self.assertEqual(storage._engine.pool.__class__.__name__, 'NullPool')
