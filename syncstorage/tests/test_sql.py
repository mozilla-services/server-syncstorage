# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from mozsvc.plugin import load_and_register
from mozsvc.tests.support import get_test_configurator

from syncstorage.tests.support import StorageTestCase
from syncstorage.storage import load_storage_from_settings

from syncstorage.tests.test_storage import StorageTestsMixin

from mozsvc.exceptions import BackendError

_UID = 1
_PLD = '*' * 500


class TestSQLStorage(StorageTestCase, StorageTestsMixin):

    # These tests need to be run with a real, file-backed sqlite database.
    # If we used an in-memory database then all threads would share a single
    # connection, and the threading/locking tests would be pointless.
    TEST_INI_FILE = "tests-filedb.ini"

    def setUp(self):
        super(TestSQLStorage, self).setUp()
        settings = self.config.registry.settings
        self.storage = load_storage_from_settings("storage", settings)

    def test_no_create(self):
        # Storage with no create_tables option; it should default to false.
        # This should fail because the table is absent
        config = get_test_configurator(__file__, 'tests-nocreate.ini')
        storage = load_and_register("storage", config)
        bsos = [{"id": "TEST", "payload": _PLD}]
        self.assertRaises(BackendError, storage.set_items, _UID, "test", bsos)

        # Storage with create_tables explicitly set to false.
        # This should fail because the table is absent
        config = get_test_configurator(__file__, 'tests-dontcreate.ini')
        storage = load_and_register("storage", config)
        self.assertRaises(BackendError, storage.set_items, _UID, "test", bsos)

        # Storage with create_tables explicit set to true.
        # This should succeed because the table gets created.
        config = get_test_configurator(__file__, 'tests-docreate.ini')
        storage = load_and_register("storage", config)
        storage.set_items(_UID, "test", bsos)

    def test_shard(self):
        # Use a configuration with sharding enabled.
        config = get_test_configurator(__file__, 'tests-shard.ini')
        storage = load_and_register("storage", config)

        # Make sure it's using the expected table name.
        table = storage.dbconnector.get_bso_table(_UID).name
        self.assertEqual(table, 'bso1')

        # The table should initially be empty.
        COUNT_ITEMS = 'select count(*) from bso1 /* queryName=COUNT_ITEMS */'
        with storage.dbconnector.connect() as c:
            res = c.execute(COUNT_ITEMS)
            self.assertEqual(res.fetchall()[0][0], 0)

        # Do a few things on the DB
        id1 = 'ec1b7457-003a-45a9-bf1c-c34e37225ad7'
        id2 = '339f52e1-deed-497c-837a-1ab25a655e37'
        storage.set_item(_UID, 'col1', id1, {'payload': _PLD})
        storage.set_item(_UID, 'col1', id2, {'payload': _PLD * 89})
        self.assertEquals(len(storage.get_items(_UID, 'col1')["items"]), 2)

        # Now make sure we did that in the right table
        with storage.dbconnector.connect() as c:
            res = c.execute(COUNT_ITEMS)
            self.assertEqual(res.fetchall()[0][0], 2)

    def test_nopool_is_disabled_when_using_memory_database(self):
        config = get_test_configurator(__file__, 'tests-nopool.ini')
        # Using no_pool=True will give you a NullPool when using file db.
        storage = load_and_register("storage-file", config)
        self.assertEqual(storage.dbconnector.engine.pool.__class__.__name__,
                         'NullPool')
        # Using no_pool=True will give you an error when using :memory: db.
        self.assertRaises(ValueError,
                          load_and_register, "storage-memory", config)
