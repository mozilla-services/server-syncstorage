# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import sys
import time
import unittest2
import subprocess

from mozsvc.exceptions import BackendError

from syncstorage.tests.support import StorageTestCase
from syncstorage.storage import (load_storage_from_settings,
                                 NotFoundError)

try:
    from syncstorage.storage.memcached import MemcachedStorage  # NOQA
    MEMCACHED = True
except ImportError:
    MEMCACHED = False


def spawn_script(name, *args, **kwds):
    scriptdir = os.path.join(os.path.dirname(__file__), "..", "scripts")
    scriptfile = os.path.join(scriptdir, name)
    command = (sys.executable, scriptfile) + args
    return subprocess.Popen(command, **kwds)


class TestMemcacheManagementScripts(StorageTestCase):

    TEST_INI_FILE = "tests-memcached.ini"

    def setUp(self):
        super(TestMemcacheManagementScripts, self).setUp()
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

    def test_mcclear_script(self):
        # Create some data in cached collections, for three different users.
        self.storage.set_item(1, "meta", "test", {"payload": "test"})
        self.storage.set_item(1, "tabs", "test", {"payload": "test"})
        self.storage.set_item(2, "meta", "test", {"payload": "test"})
        self.storage.set_item(2, "tabs", "test", {"payload": "test"})
        self.storage.set_item(3, "meta", "test", {"payload": "test"})
        self.storage.set_item(3, "tabs", "test", {"payload": "test"})
        # Initially, they should all have some data in memcache.
        self.assertTrue(self.storage.cache.get("1:metadata"))
        self.assertTrue(self.storage.cache.get("2:metadata"))
        self.assertTrue(self.storage.cache.get("3:metadata"))
        # Run the mcclear script on users 2 and 3.
        ini_file = os.path.join(os.path.dirname(__file__), self.TEST_INI_FILE)
        proc = spawn_script("mcclear.py", ini_file, stdin=subprocess.PIPE)
        proc.stdin.write("2\n\n3\n")
        proc.stdin.close()
        assert proc.wait() == 0
        # Those users should have no data in memcache, with user 1 unaffected.
        self.assertTrue(self.storage.cache.get("1:metadata"))
        self.assertFalse(self.storage.cache.get("2:metadata"))
        self.assertFalse(self.storage.cache.get("3:metadata"))
        # They should also have lost their tabs, which exist only in memcache.
        self.assertTrue(self.storage.get_item(1, "tabs", "test"))
        self.assertRaises(NotFoundError,
                          self.storage.get_item, 2, "tabs", "test")
        self.assertRaises(NotFoundError,
                          self.storage.get_item, 3, "tabs", "test")
        # But all meta items should be intact, because DB.
        self.assertTrue(self.storage.get_item(1, "meta", "test"))
        self.assertTrue(self.storage.get_item(2, "meta", "test"))
        self.assertTrue(self.storage.get_item(3, "meta", "test"))

    def test_mcread_script(self):
        # Create some data in cached collections, for three different users.
        self.storage.set_item(1, "tabs", "test1", {"payload": "test1"})
        self.storage.set_item(2, "tabs", "test2", {"payload": "test2"})
        self.storage.set_item(3, "tabs", "test3", {"payload": "test3"})
        # Run the mcread script on users 2 and 3.
        ini_file = os.path.join(os.path.dirname(__file__), self.TEST_INI_FILE)
        proc = spawn_script("mcread.py", ini_file,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)
        proc.stdin.write("2\n\n3\n")
        proc.stdin.close()
        output = [ln.strip() for ln in proc.stdout]
        assert proc.wait() == 0
        # There should be 4 items, two for each queried user.
        self.assertEquals(len(output), 4)
        output_keys = [ln.split()[0] for ln in output]
        self.assertTrue("2:metadata" in output_keys)
        self.assertTrue("2:c:tabs" in output_keys)
        self.assertTrue("3:metadata" in output_keys)
        self.assertTrue("3:c:tabs" in output_keys)


class TestPurgeTTLScript(StorageTestCase):

    TEST_INI_FILE = "tests-hostname.ini"

    def test_purgettl_script(self):
        # Use a non-default storage, to test if it hits all backends.
        key = "syncstorage:storage:host:another-test-host"
        storage = self.config.registry[key]

        def count_items():
            COUNT_ITEMS = "select count(*) from %(bso)s "\
                          "/* queryName=COUNT_ITEMS */"
            total_items = 0
            for i in xrange(storage.dbconnector.shardsize):
                with storage.dbconnector.connect() as c:
                    res = c.execute(COUNT_ITEMS % {"bso": "bso" + str(i)})
                    total_items += res.fetchall()[0][0]
            return total_items

        storage.set_item(1, "col", "test1", {"payload": "X", "ttl": 0})
        storage.set_item(1, "col", "test2", {"payload": "X", "ttl": 0})
        storage.set_item(1, "col", "test3", {"payload": "X", "ttl": 30})
        self.assertEquals(count_items(), 3)

        time.sleep(1)

        # Long grace period == not purged
        ini_file = os.path.join(os.path.dirname(__file__), self.TEST_INI_FILE)
        proc = spawn_script("purgettl.py",
                            "--oneshot",
                            "--backend-interval=0",
                            "--grace-period=30",
                            ini_file)
        assert proc.wait() == 0
        self.assertEquals(count_items(), 3)

        # Short grace period == not purged
        ini_file = os.path.join(os.path.dirname(__file__), self.TEST_INI_FILE)
        proc = spawn_script("purgettl.py",
                            "--oneshot",
                            "--backend-interval=0",
                            "--grace-period=0",
                            ini_file)
        assert proc.wait() == 0
        self.assertEquals(count_items(), 1)
