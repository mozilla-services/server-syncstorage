# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import time
import threading

from mozsvc.plugin import load_and_register
from mozsvc.tests.support import get_test_configurator

from syncstorage.tests.support import StorageTestCase
from syncstorage.storage import load_storage_from_settings
from syncstorage.storage.sql.dbconnect import (create_engine,
                                               QueuePoolWithMaxBacklog)

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

    def test_max_overflow_and_max_backlog(self):
        # Create an engine with known pool parameters.
        # Unfortunately we can't load this from a config file, since
        # pool params are ignored for sqlite databases.
        engine = create_engine(
            "sqlite:///:memory:",
            poolclass=QueuePoolWithMaxBacklog,
            pool_size=2,
            pool_timeout=1,
            max_backlog=2,
            max_overflow=1)

        # Define a utility function to take a connection from the pool
        # and hold onto it.  This makes it easy to spawn as a bg thread
        # and test blocking/timeout behaviour.
        connections = []
        errors = []

        def take_connection():
            try:
                connections.append(engine.connect())
            except Exception, e:
                errors.append(e)

        # The size of the pool is two, so we can take
        # two connections right away without any error.
        take_connection()
        take_connection()
        self.assertEquals(len(connections), 2)
        self.assertEquals(len(errors), 0)

        # The pool allows an overflow of 1, so we can
        # take another, ephemeral connection without any error.
        take_connection()
        self.assertEquals(len(connections), 3)
        self.assertEquals(len(errors), 0)

        # The pool allows a backlog of 2, so we can
        # spawn two threads that will block waiting for a connection.
        thread1 = threading.Thread(target=take_connection)
        thread1.start()
        thread2 = threading.Thread(target=take_connection)
        thread2.start()
        time.sleep(0.1)
        self.assertEquals(len(connections), 3)
        self.assertEquals(len(errors), 0)

        # The pool is now exhausted and at maximum backlog.
        # Trying to take another connection fails immediately.
        t1 = time.time()
        take_connection()
        t2 = time.time()
        self.assertEquals(len(connections), 3)
        # This checks that it failed immediately rather than timing out.
        self.assertTrue(t2 - t1 < 0.9)
        self.assertTrue(len(errors) >= 1)

        # And eventually, the blocked threads will time out.
        thread1.join()
        thread2.join()
        self.assertEquals(len(connections), 3)
        self.assertEquals(len(errors), 3)

    def test_purging_of_expired_items(self):

        def count_items():
            COUNT_ITEMS = "select count(*) from bso "\
                          "/* queryName=COUNT_ITEMS */"
            with self.storage.dbconnector.connect() as c:
                res = c.execute(COUNT_ITEMS)
                return res.fetchall()[0][0]

        # Initially there should be no entries in the db.
        self.assertEquals(count_items(), 0)

        # Add 2000 items with short ttl to the db.
        # This forces the purge script to run several iterations.
        items = [{"id": "SHORT" + str(i), "payload": str(i), "ttl": 0}
                 for i in xrange(2000)]
        self.storage.set_items(_UID, "col", items)

        # Add 5 items with long ttl to the db.
        items = [{"id": "LONG" + str(i), "payload": str(i), "ttl": 10}
                 for i in xrange(5)]
        self.storage.set_items(_UID, "col", items)

        # Wait for ttls to expire.
        # The items should be in the database, but not read by the backend.
        time.sleep(1)
        self.assertEquals(count_items(), 2005)
        self.assertEquals(len(self.storage.get_items(_UID, "col")["items"]), 5)

        # Purging with a long grace period will not remove them.
        res = self.storage.purge_expired_items(grace_period=100)
        self.assertEquals(res["num_purged"], 0)
        self.assertEquals(count_items(), 2005)
        self.assertEquals(len(self.storage.get_items(_UID, "col")["items"]), 5)

        # Purging with no grace period should remove them from the database.
        res = self.storage.purge_expired_items(grace_period=0)
        self.assertEquals(res["num_purged"], 2000)
        self.assertEquals(count_items(), 5)
        self.assertEquals(len(self.storage.get_items(_UID, "col")["items"]), 5)
