# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from syncstorage import main, tweens
from syncstorage.storage import get_storage
from syncstorage.tests.support import StorageTestCase


class TestWSGIApp(StorageTestCase):

    def test_host_specific_config(self):
        req = self.make_request(environ={"HTTP_HOST": "localhost"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/tests.db")
        req = self.make_request(environ={"HTTP_HOST": "some-test-host"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/some-test-host.db")
        req = self.make_request(environ={"HTTP_HOST": "some-test-host:8000"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/some-test-host.db")
        req = self.make_request(environ={"HTTP_HOST": "another-test-host"})
        self.assertEquals(get_storage(req).sqluri,
                          "sqlite:////tmp/another-test-host.db")

    def test_dependant_options(self):
        # make sure the app cannot be initialized if it's asked
        # to check for blacklisted node and memcached is not present
        settings = self.config.registry.settings.copy()
        settings['storage.check_blacklisted_nodes'] = True
        old_client = tweens.Client
        tweens.Client = None
        try:
            self.assertRaises(ValueError, main, {}, **settings)
        finally:
            tweens.Client = old_client
