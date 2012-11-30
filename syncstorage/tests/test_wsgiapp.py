# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from webtest import TestApp

from syncstorage import main, tweens
from syncstorage.storage import get_storage
from syncstorage.tests.support import StorageTestCase


class FakeMemcachedClient(object):
    """A simple in-memory fake for the MemcachedClient class."""

    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key, None)

    def set(self, key, value):
        self.values[key] = value


class TestWSGIApp(StorageTestCase):

    TEST_INI_FILE = "tests-hostname.ini"

    def test_host_specific_config(self):
        req = self.make_request(environ={"HTTP_HOST": "localhost"})
        sqluri = get_storage(req).sqluri
        self.assertEquals(sqluri, "sqlite:///:memory:")

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
        # to check for node status and memcached is not present
        settings = self.config.registry.settings.copy()
        settings['storage.check_node_status'] = True
        old_client = tweens.MemcachedClient
        tweens.MemcachedClient = None
        try:
            self.assertRaises(ValueError, main, {}, **settings)
        finally:
            tweens.MemcachedClient = old_client

    def test_checking_node_status_in_memcache(self):
        config = self.config

        app = TestApp(config.make_wsgi_app(), extra_environ={
            "HTTP_HOST": "some-test-host",
        })

        cache = FakeMemcachedClient()
        config.registry["check_node_status:cache"] = cache

        # The app automatically fuzzes the backoff value in headers, so
        # we can't use a straight assertEquals to check it.
        def assertHasBackoffHeader(response, header="X-Backoff", value=None):
            if value is None:
                value = config.registry.settings["mozsvc.retry_after"]
            got_value = int(response.headers[header])
            if not value <= got_value <= value + 5:
                msg = "Backoff of %d does not match %d" % (got_value, value)
                assert False, msg

        # With no node data in memcache, requests to known nodes should
        # succeed while requests to unknown nodes should fail.
        app.get("/__heartbeat__", headers={"Host": "some-test-host"},
                status=200)
        app.get("/__heartbeat__", headers={"Host": "unknown-host"},
                status=503)

        # Marking the node as "backoff" will succeed, but send backoff header.
        cache.set("status:some-test-host", "backoff")
        r = app.get("/__heartbeat__", status=200)
        assertHasBackoffHeader(r, "X-Backoff")

        cache.set("status:some-test-host", "backoff:100")
        r = app.get("/__heartbeat__", status=200)
        assertHasBackoffHeader(r, "X-Backoff", 100)

        # Marking the node as "down", "draining" or "unhealthy" will result
        # in a 503 response with backoff header.
        cache.set("status:some-test-host", "down")
        r = app.get("/__heartbeat__", status=503)
        assertHasBackoffHeader(r, "X-Backoff")
        assertHasBackoffHeader(r, "Retry-After")

        cache.set("status:some-test-host", "draining")
        r = app.get("/__heartbeat__", status=503)
        assertHasBackoffHeader(r, "X-Backoff")
        assertHasBackoffHeader(r, "Retry-After")

        cache.set("status:some-test-host", "unhealthy")
        r = app.get("/__heartbeat__", status=503)
        assertHasBackoffHeader(r, "X-Backoff")
        assertHasBackoffHeader(r, "Retry-After")

        # A nonsense node status will be ignored.
        cache.set("status:some-test-host", "nonsensical-value")
        r = app.get("/__heartbeat__", status=200)
        self.assertTrue("X-Backoff" not in r.headers)

        # Node status only affects the node that it belongs to.
        cache.set("status:some-test-host", "unhealthy")
        r = app.get("/__heartbeat__",
                    headers={"Host": "another-test-host"},
                    status=200)
        self.assertTrue("X-Backoff" not in r.headers)
