# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest
import os

from syncstorage.wsgiapp import make_app


class TestWSGIApp(unittest.TestCase):

    def setUp(self):
        config_file = os.path.join(os.path.dirname(__file__), "sync.conf")
        self.app = make_app({"configuration": "file:" + config_file}).app

    def test_host_specific_config(self):
        class request:
            host = "localhost"
        self.assertEquals(self.app.get_storage(request).sqluri,
                          "sqlite:////tmp/tests.db")
        request.host = "some-test-host"
        self.assertEquals(self.app.get_storage(request).sqluri,
                          "sqlite:////tmp/some-test-host.db")
        request.host = "another-test-host"
        self.assertEquals(self.app.get_storage(request).sqluri,
                          "sqlite:////tmp/another-test-host.db")
