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


class TestWsgiApp(unittest.TestCase):

    def setUp(self):
        # loading the app
        self.appdir, self.config, self.storage, self.auth = initenv()
        # we don't support other storages for this test
        assert self.storage.sqluri.split(':/')[0] in ('mysql', 'sqlite')
        self.sqlfile = self.storage.sqluri.split('sqlite:///')[-1]
        self.app = TestApp(make_app(self.config))

        # adding a user if needed
        self.user_name = 'test_user_%d' % random.randint(1, 100000)
        self.password = 'x' * 9
        self.auth.create_user(self.user_name, self.password,
                              'tarek@mozilla.com')
        self.user_id = self.auth.get_user_id(self.user_name)

    def tearDown(self):
        self.storage.delete_storage(self.user_id)
        if not self.auth.delete_user(self.user_id, self.password):
            raise ValueError('Could not remove user "%s"' % self.user_name)

        cef_logs = os.path.join(self.appdir, 'test_cef.log')
        if os.path.exists(cef_logs):
            os.remove(cef_logs)

        if os.path.exists(self.sqlfile):
            os.remove(self.sqlfile)
        else:
            self.auth._engine.execute('truncate users')
            self.auth._engine.execute('truncate collections')
            self.auth._engine.execute('truncate wbo')
