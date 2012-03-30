# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import urlparse

from mozsvc.tests.support import TestCase
from mozsvc.metrics import setup_metlog, teardown_metlog


class StorageTestCase(TestCase):
    """TestCase class with automatic cleanup of database files."""

    def tearDown(self):
        self._cleanup_test_databases()
        teardown_metlog()
        super(StorageTestCase, self).tearDown()

    def get_test_configurator(self):
        config = super(StorageTestCase, self).get_test_configurator()
        setup_metlog(config.registry.settings.getsection('metlog'))
        config.include("syncstorage")
        return config

    def _cleanup_test_databases(self):
        """Clean up any database used during the tests."""
        # Find any in-use mysql database and truncate the tables.
        for key, storage in self.config.registry.iteritems():
            if not key.startswith("syncstorage:storage:"):
                continue
            if "mysql" in storage.sqluri:
                storage._engine.execute('truncate collections')
                storage._engine.execute('truncate bso')
        # Find any sqlite database files and delete them.
        for key, value in self.config.registry.settings.iteritems():
            if key.endswith(".sqluri"):
                sqluri = urlparse.urlparse(value)
                if sqluri.scheme == 'sqlite':
                    if os.path.exists(sqluri.path):
                        os.remove(sqluri.path)
