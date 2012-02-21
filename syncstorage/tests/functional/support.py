# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
""" Base test class, with an instanciated app.
"""

import os
import unittest
import urlparse
import random

from webtest import TestApp

from mozsvc.tests.support import get_test_configurator

from syncstorage.tokens import ServicesTokenManager


class TestWsgiApp(unittest.TestCase):

    def setUp(self):
        self.config = get_test_configurator(__file__)
        self.config.include("syncstorage")

        # We only support mysql and sqlite databases.
        # Check that the config keys match this expectation.
        # Also get a list of temp database files to delete on cleanup.
        self.sqlfiles = []
        for key, value in self.config.registry.settings.iteritems():
            if key.endswith(".sqluri"):
                sqluri = urlparse.urlparse(value)
                assert sqluri.scheme in ('mysql',  'sqlite')
                if sqluri.scheme == 'sqlite':
                    self.sqlfiles.append(sqluri.path)

        self.app = TestApp(self.config.make_wsgi_app())

        # adding a user if needed
        self.user_email = "test_%d@example.com" % random.randint(1, 100000)
        user = ServicesTokenManager.get_user_data(self.user_email)
        self.user_name = user["username"]
        self.user_id = user["userid"]

    def tearDown(self):
        for key, storage in self.config.registry.iteritems():
            if not key.startswith("storage:"):
                continue
            storage.delete_storage(self.user_id)
            if "mysql" in storage.sqluri:
                storage._engine.execute('truncate collections')
                storage._engine.execute('truncate wbo')

        for sqlfile in self.sqlfiles:
            if os.path.exists(sqlfile):
                os.remove(sqlfile)
