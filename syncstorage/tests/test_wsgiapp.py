# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest

from mozsvc.metrics import setup_metlog
from mozsvc.tests.support import get_test_configurator, make_request

from syncstorage.storage import get_storage


class TestWSGIApp(unittest.TestCase):

    def setUp(self):
        self.config = get_test_configurator(__file__)
        setup_metlog(self.config.registry.settings.getsection('metlog'))
        self.config.include("syncstorage")

    def _make_request(self, *args, **kwds):
        return make_request(self.config, *args, **kwds)

    def test_batch_size(self):
        # check that the batch size is correctly set
        size = self.config.registry["syncstorage.controller"].batch_size
        self.assertEqual(size, 25)

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

    def test_dependant_options(self):
        # make sure the app cannot be initialized if it's asked
        # to check for blacklisted node and memcached is not present
        settings = self.config.registry.settings.copy()
        settings['storage.check_blacklisted_nodes'] = True
        from syncstorage import main, tweens
        old_client = tweens.Client
        tweens.Client = None
        try:
            self.assertRaises(ValueError, main, {}, **settings)
        finally:
            tweens.Client = old_client
