# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest
import os
import time

from syncstorage.tests.support import initenv
from syncstorage.storage.sqlmappers import get_wbo_table_name
from syncstorage.storage import SyncStorage
from syncstorage.storage.sql import SQLStorage
SyncStorage.register(SQLStorage)

from services.auth import ServicesAuth
from services.auth.sql import SQLAuth
from services.util import BackendError
ServicesAuth.register(SQLAuth)

_UID = 1
_PLD = '*' * 500


class TestSQLStorage(unittest.TestCase):

    def setUp(self):
        self.appdir, self.config, self.storage, self.auth = initenv()
        # we don't support other storages for this test
        assert self.storage.sqluri.split(':/')[0] in ('mysql', 'sqlite',
                                                      'pymysql')

        self.sqlfile = self.storage.sqluri.split('sqlite:///')[-1]
        # make sure we have the standard collections in place
        for name in ('client', 'crypto', 'forms', 'history', 'key', 'meta',
                     'bookmarks', 'prefs', 'tabs', 'passwords'):
            self.storage.set_collection(_UID, name)

        self._cfiles = []

    def tearDown(self):
        self._del_db()
        for file_ in self._cfiles:
            if os.path.exists(file_):
                os.remove(file_)

    def _add_cleanup(self, path):
        self._cfiles.append(path)

    def _del_db(self):
        if os.path.exists(self.sqlfile):
            os.remove(self.sqlfile)
        else:
            self.storage._engine.execute('truncate users')
            self.storage._engine.execute('truncate collections')
            self.storage._engine.execute('truncate wbo')

    def test_user_exists(self):
        self.assertFalse(self.storage.user_exists(_UID))

    def test_set_get_user(self):
        self.assertFalse(self.storage.user_exists(_UID))
        self.storage.set_user(_UID, username='tarek', email='tarek@ziade.org')
        self.assertTrue(self.storage.user_exists(_UID))
        self.storage.set_user(_UID, email='tarek2@ziade.org')
        res = self.storage.get_user(_UID, fields=['email'])
        self.assertEquals(res, (u'tarek2@ziade.org',))
        res = self.storage.get_user(_UID)
        self.assertEquals(res, (1, u'tarek', None, u'tarek2@ziade.org', 0,
                                None, None, None))

    def test_collections(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.assertFalse(self.storage.collection_exists(_UID, 'My collection'))
        self.storage.set_collection(_UID, 'My collection')
        self.assertTrue(self.storage.collection_exists(_UID, 'My collection'))

        res = dict(self.storage.get_collection(_UID, 'My collection').items())
        self.assertEqual(res['name'], 'My collection')
        self.assertEqual(res['userid'], 1)
        res = self.storage.get_collection(_UID, 'My collection',
                                          fields=['name'])
        self.assertEquals(res, {'name': 'My collection'})

        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 11)
        res = dict(res[-1].items())
        self.assertEqual(res['name'], 'My collection')
        self.assertEqual(res['userid'], 1)

        res = self.storage.get_collections(_UID, fields=['name'])
        res = [line[0] for line in res]
        self.assertTrue('My collection' in res)

        # adding a new collection
        self.storage.set_collection(_UID, 'My collection 2')
        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 12)

        names = self.storage.get_collection_names(_UID)
        self.assertEquals([name[1] for name in names[-2:]],
                          ['My collection', 'My collection 2'])

        # removing a collection
        self.storage.delete_collection(_UID, 'My collection 2')
        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 11)

        # removing *all*
        self.storage.delete_storage(_UID)
        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 0)
        self.storage.delete_user(_UID)
        self.assertFalse(self.storage.user_exists(_UID))

    def test_items(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col')
        self.assertFalse(self.storage.item_exists(_UID, 'col', 1))
        self.assertEquals(self.storage.get_items(_UID, 'col'), [])

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
        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 0)

        self.storage.set_items(_UID, 'col',
                               items=[{'id': 'o', 'payload': _PLD}])
        res = self.storage.get_item(_UID, 'col', 'o')
        self.assertEquals(res['payload'], _PLD)

    def test_get_collection_timestamps(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col1')
        self.storage.set_collection(_UID, 'col2')
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col2', 1, payload=_PLD)

        timestamps = self.storage.get_collection_timestamps(_UID)
        names = timestamps.keys()
        self.assertTrue('col1' in names)
        self.assertTrue('col2' in names)
        col1 = self.storage.get_collection_max_timestamp(_UID, 'col2')
        self.assertAlmostEquals(col1, timestamps['col2'])

        # check that when we have several users, the method
        # still returns the same timestamps for the first user
        # which differs from the second user
        time.sleep(1.)
        self.storage.set_user(2, email='tarek2@ziade.org')
        self.storage.set_collection(2, 'col1')
        self.storage.set_collection(2, 'col2')
        self.storage.set_item(2, 'col1', 1, payload=_PLD)
        self.storage.set_item(2, 'col2', 1, payload=_PLD)

        user1_timestamps = self.storage.get_collection_timestamps(_UID)
        user1_timestamps = user1_timestamps.items()
        user1_timestamps.sort()

        user2_timestamps = self.storage.get_collection_timestamps(2)
        user2_timestamps = user2_timestamps.items()
        user2_timestamps.sort()

        self.assertNotEqual(user1_timestamps, user2_timestamps)

    def test_storage_size(self):
        before = self.storage.get_total_size(_UID)
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col1')
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col1', 2, payload=_PLD)
        wanted = len(_PLD) * 2 / 1024.
        self.assertEquals(self.storage.get_total_size(_UID) - before, wanted)

    def test_ttl(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col1')
        self.storage.set_item(_UID, 'col1', 1, payload=_PLD)
        self.storage.set_item(_UID, 'col1', 2, payload=_PLD, ttl=0)
        time.sleep(1.1)
        self.assertEquals(len(self.storage.get_items(_UID, 'col1')), 1)
        self.assertEquals(len(self.storage.get_items(_UID, 'col1',
                                                filters={'ttl': ('>', -1)})),
                                                2)

    def test_dashed_ids(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col1')
        id1 = '{ec1b7457-003a-45a9-bf1c-c34e37225ad7}'
        id2 = '{339f52e1-deed-497c-837a-1ab25a655e37}'
        self.storage.set_item(_UID, 'col1', id1, payload=_PLD)
        self.storage.set_item(_UID, 'col1', id2, payload=_PLD * 89)
        self.assertEquals(len(self.storage.get_items(_UID, 'col1')), 2)

        # now trying to delete them
        self.storage.delete_items(_UID, 'col1', item_ids=[id1, id2])
        self.assertEquals(len(self.storage.get_items(_UID, 'col1')), 0)

    def test_no_create(self):
        # testing the create_tables option
        testsdir = os.path.dirname(__file__)

        # when not provided it is not created
        conf = os.path.join(testsdir, 'tests3.ini')
        appdir, config, storage, auth = initenv(conf)

        # this should fail because the table is absent
        self.assertRaises(BackendError, storage.set_user, _UID,
                          email='tarek@ziade.org')

        # create_table = false
        conf = os.path.join(testsdir, 'tests4.ini')
        appdir, config, storage, auth = initenv(conf)
        sqlfile = storage.sqluri.split('sqlite:///')[-1]
        try:
            # this should fail because the table is absent
            self.assertRaises(BackendError, storage.set_user, _UID,
                              email='tarek@ziade.org')
        finally:
            # removing the db created
            if os.path.exists(sqlfile):
                os.remove(sqlfile)

        # create_table = true
        conf = os.path.join(testsdir, 'tests2.ini')
        appdir, config, storage, auth = initenv(conf)

        # this should work because the table is absent
        storage.set_user(_UID, email='tarek@ziade.org')

    def test_shard(self):
        self._add_cleanup(os.path.join('/tmp', 'tests2.db'))

        # make shure we do shard
        testsdir = os.path.dirname(__file__)
        conf = os.path.join(testsdir, 'tests2.ini')

        appdir, config, storage, auth = initenv(conf)

        res = storage._engine.execute('select count(*) from wbo1')
        self.assertEqual(res.fetchall()[0][0], 0)

        # doing a few things on the DB
        storage.set_user(_UID, email='tarek@ziade.org')
        storage.set_collection(_UID, 'col1')
        id1 = '{ec1b7457-003a-45a9-bf1c-c34e37225ad7}'
        id2 = '{339f52e1-deed-497c-837a-1ab25a655e37}'
        storage.set_item(_UID, 'col1', id1, payload=_PLD)
        storage.set_item(_UID, 'col1', id2, payload=_PLD * 89)
        self.assertEquals(len(storage.get_items(_UID, 'col1')), 2)

        # now making sure we did that in the right table
        table = get_wbo_table_name(_UID)
        self.assertEqual(table, 'wbo1')
        res = storage._engine.execute('select count(*) from wbo1')
        self.assertEqual(res.fetchall()[0][0], 2)

    def test_nopool(self):
        # make sure the pool is forced to NullPool when sqlite is used.
        testsdir = os.path.dirname(__file__)
        conf = os.path.join(testsdir, 'tests2.ini')

        appdir, config, storage, auth = initenv(conf)
        self.assertEqual(storage._engine.pool.__class__.__name__, 'NullPool')


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
