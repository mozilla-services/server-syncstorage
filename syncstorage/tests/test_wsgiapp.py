# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest

from mozsvc.tests.support import get_test_configurator, make_request

from syncstorage.storage import get_storage


class TestWSGIApp(unittest.TestCase):

    def setUp(self):
        self.config = get_test_configurator(__file__)
        self.config.include("syncstorage")

    def _make_request(self, *args, **kwds):
        return make_request(self.config, *args, **kwds)

    def test_host_specific_config(self):
        req = self._make_request(environ={"HTTP_HOST": "localhost"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/tests.db")
        req = self._make_request(environ={"HTTP_HOST": "some-test-host"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/some-test-host.db")
        req = self._make_request(environ={"HTTP_HOST": "another-test-host"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/another-test-host.db")
