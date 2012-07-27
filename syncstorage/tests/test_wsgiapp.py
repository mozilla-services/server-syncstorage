# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from syncstorage import main, tweens
from syncstorage.storage import get_storage
from syncstorage.tests.support import StorageTestCase


class TestWSGIApp(StorageTestCase):

    def test_host_specific_config(self):
        req = self.make_request(environ={"HTTP_HOST": "localhost"})
        sqluri = get_storage(req).sqluri
        self.assertTrue(sqluri.startswith("sqlite:////tmp/tests-"))

        req = self.make_request(environ={"HTTP_HOST": "some-test-host"})
        sqluri = get_storage(req).sqluri
        self.assertTrue(sqluri.startswith("sqlite:////tmp/some-test-host-"))

        req = self.make_request(environ={"HTTP_HOST": "some-test-host:8000"})
        sqluri = get_storage(req).sqluri
        self.assertTrue(sqluri.startswith("sqlite:////tmp/some-test-host-"))

        req = self.make_request(environ={"HTTP_HOST": "another-test-host"})
        sqluri = get_storage(req).sqluri
        self.assertTrue(sqluri.startswith("sqlite:////tmp/another-test-host-"))

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
