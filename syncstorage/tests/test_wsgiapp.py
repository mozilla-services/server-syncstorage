# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from pyramid.security import IAuthenticationPolicy
from pyramid.httpexceptions import HTTPUnauthorized
import hawkauthlib

from webtest import TestApp
import testfixtures

from mozsvc.user import RequestWithUser as Request

from syncstorage.storage import get_storage
from syncstorage.storage.sql.dbconnect import MigrationState
from syncstorage.tests.support import StorageTestCase


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

    def _make_test_app(self, user=None):
        app = TestApp(self.config.make_wsgi_app())

        # Monkey-patch the app to make legitimate hawk-signed requests.
        if user is None:
            user = {}
        user_id = user.get('uid', 42)
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        req = Request.blank("http://localhost/")
        auth_token, auth_secret = auth_policy.encode_hawk_id(req, user_id,
                                                             user)

        def new_do_request(req, *args, **kwds):
            hawkauthlib.sign_request(req, auth_token, auth_secret)
            return orig_do_request(req, *args, **kwds)

        orig_do_request = app.do_request
        app.do_request = new_do_request

        return app

    def _make_signed_req(self, userid, user):
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        req = Request.blank("http://localhost/")
        req.registry = self.config.registry
        auth_token, auth_secret = auth_policy.encode_hawk_id(req, userid, user)
        hawkauthlib.sign_request(req, auth_token, auth_secret)
        req.metrics = {}
        return req

    def test_the_it_works_page(self):
        app = self._make_test_app()
        r = app.get("/")
        self.assertTrue("It Works!" in r.body)

    def test_lbheartbeat_page(self):
        app = self._make_test_app()
        r = app.get("/__lbheartbeat__")
        self.assertEqual({}, r.json)

    def test_metrics_capture(self):
        app = self._make_test_app()

        # Make a request that hits the database, capturing its logs.
        with testfixtures.LogCapture() as logs:
            app.get("/1.5/42/info/collections")

        # DB usage metrics should have been generated in a log message.
        for r in logs.records:
            if "syncstorage.storage.sql.db.execute" in r.__dict__:
                break
        else:
            assert False, "metrics were not collected"

    def test_logging_of_invalid_bsos(self):
        app = self._make_test_app()

        # Make a request with an invalid bso, capturing its logs
        with testfixtures.LogCapture() as logs:
            app.post_json("/1.5/42/storage/bookmarks", [
                {"id": "valid1", "payload": "thisisok"},
                {"id": "invalid", "payload": "TOOBIG" * 1024 * 1024 * 3},
                {"id": "valid2", "payload": "thisisoktoo"},
            ])

        # An error log should have been generated
        for r in logs.records:
            if r.name == "syncstorage.views.validators":
                expect_message = "Invalid BSO 42/bookmarks/invalid" \
                    " (payload too large):" \
                    " BSO({\"id\": \"invalid\", \"payload_size\": 18874368})"
                self.assertEqual(r.getMessage(), expect_message)
                break
        else:
            assert False, "log was not generated"

    def test_metrics_capture_for_batch_uploads(self):
        app = self._make_test_app()

        collection = "/1.5/42/storage/xxx_col1"

        with testfixtures.LogCapture() as logs:
            bso = {"id": "1", "payload": "x"}
            res = app.post_json(collection + "?batch=true", [bso])
            batch = res.json["batch"]

        for r in logs.records:
            if "syncstorage.storage.sql.append_items_to_batch" in r.__dict__:
                break
        else:
            assert False, "timer metrics were not emitted"

        with testfixtures.LogCapture() as logs:
            endpoint = collection + "?batch={0}&commit=true".format(batch)
            app.post_json(endpoint, [])

        # DB timing metrics should have been generated in a log message.
        for r in logs.records:
            if "syncstorage.storage.sql.apply_batch" in r.__dict__:
                break
        else:
            assert False, "timer metrics were not emitted"

    def test_503s_for_migrating_users(self):
        user = {
            "uid": 42,
            "fxa_uid": "foobar",
            "fxa_kid": "001122",
            "hashed_fxa_uid": "aabbcc",
        }
        app = self._make_test_app(user)
        req = self._make_signed_req(user["uid"], user)

        # There's no public API for marking a user as migrated.
        storage = get_storage(req)
        with storage.dbconnector.connect() as connection:
            connection.execute("""
                INSERT INTO migration (fxa_uid, started_at, state)
                VALUES (:fxa_uid, :started_at, :state)
                /* queryName=migrate */
            """, params={
                "fxa_uid": "foobar",
                "started_at": 12345,
                "state": MigrationState.IN_PROGRESS,
            })

        res = app.get("/1.5/42/info/collections", status=503)
        self.assertTrue("Retry-After" in res.headers)

    def test_receiving_old_style_fxa_uid_in_auth_token(self):
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        req = self._make_signed_req(42, {
            "fxa_uid": "hashed-uid",
            "device_id": "hashed-device-id",
        })

        self.assertEquals(auth_policy.authenticated_userid(req), 42)

        self.assertEquals(req.metrics["metrics_uid"], "hashed-uid")
        self.assertEquals(req.metrics["metrics_device_id"], "hashed-device-id")

        self.assertNotIn("fxa_uid", req.user)
        self.assertNotIn("fxa_kid", req.user)

    def test_receiving_new_style_fxa_uid_in_auth_token(self):
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        req = self._make_signed_req(42, {
            "fxa_uid": "raw-uid",
            "fxa_kid": "raw-new_kI-d",
            "hashed_fxa_uid": "hashed-uid",
            "hashed_device_id": "hashed-device-id",
        })

        self.assertEquals(auth_policy.authenticated_userid(req), 42)

        self.assertEquals(req.metrics["metrics_uid"], "hashed-uid")
        self.assertEquals(req.metrics["metrics_device_id"], "hashed-device-id")

        self.assertEquals(req.user["fxa_uid"], "raw-uid")
        self.assertEquals(req.user["fxa_kid"], "raw-new_kI-d")

    def test_validation_of_user_data_from_token(self):
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        check_auth = auth_policy.authenticated_userid

        req = self._make_signed_req(42, {
            "fxa_uid": "invalid\nuid",
            "fxa_kid": "raw-kid",
            "hashed_fxa_uid": "hashed-uid",
            "hashed_device_id": "hashed-device-id",
        })
        self.assertRaises(HTTPUnauthorized, check_auth, req)

        req = self._make_signed_req(42, {
            "fxa_uid": "raw-uid",
            "fxa_kid": "invalid!kid",
            "hashed_fxa_uid": "hashed-uid",
            "hashed_device_id": "hashed-device-id",
        })
        self.assertRaises(HTTPUnauthorized, check_auth, req)

        req = self._make_signed_req(42, {
            "uid": "invalid string userid",
            "fxa_uid": "raw-uid",
            "fxa_kid": "raw-kid",
            "hashed_fxa_uid": "hashed-uid",
            "hashed_device_id": "hashed-device-id",
        })
        self.assertRaises(HTTPUnauthorized, check_auth, req)
