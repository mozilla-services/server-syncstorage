# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
""" Base test class, with an instanciated app.
"""
import os
import unittest
import random

from webtest import TestApp

from syncstorage.tests.support import initenv
from syncstorage.wsgiapp import make_app
from syncstorage.tokens import ServicesTokenManager


class TestWsgiApp(unittest.TestCase):

    def setUp(self):
        # loading the app
        self.appdir, self.config, self.storage = initenv()
        # we don't support other storages for this test
        assert self.storage.sqluri.split(':/')[0] in ('mysql', 'sqlite')
        self.sqlfile = self.storage.sqluri.split('sqlite:///')[-1]
        self.app = TestApp(make_app(self.config))

        # adding a user if needed
        self.user_email = "test_%d@example.com" % random.randint(1, 100000)
        user = ServicesTokenManager.get_user_data(self.user_email)
        self.user_name = user["username"]
        self.user_id = user["userid"]

    def tearDown(self):
        self.storage.delete_storage(self.user_id)

        cef_logs = os.path.join(self.appdir, 'test_cef.log')
        if os.path.exists(cef_logs):
            os.remove(cef_logs)

        if os.path.exists(self.sqlfile):
            os.remove(self.sqlfile)
        else:
            self.storage._engine.execute('truncate collections')
            self.storage._engine.execute('truncate wbo')
