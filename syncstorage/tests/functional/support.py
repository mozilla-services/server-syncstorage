# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
""" Base test class, with an instanciated app.
"""

import os
import sys
import optparse
import random
import json
import urlparse

import unittest2

import macauthlib

from pyramid.request import Request

from mozsvc.tests.support import FunctionalTestCase
from mozsvc.user.whoauth import SagradaMACAuthPlugin

from syncstorage.tests.support import StorageTestCase


class StorageFunctionalTestCase(FunctionalTestCase, StorageTestCase):
    """Abstract base class for functional testing of a storage API."""

    def setUp(self):
        super(StorageFunctionalTestCase, self).setUp()

        # Generate userid and auth token crednentials.
        # This can be overridden by subclasses.
        self._authenticate()

        # Monkey-patch the app to sign all requests with the token.
        def new_do_request(req, *args, **kwds):
            macauthlib.sign_request(req, self.auth_token, self.auth_secret)
            return orig_do_request(req, *args, **kwds)
        orig_do_request = self.app.do_request
        self.app.do_request = new_do_request

    def _authenticate(self):
        # For basic testing, use a random uid and sign our own tokens.
        # Subclasses might like to override this and use a live tokenserver.
        self.user_id = random.randint(1, 100000)
        settings = self.config.registry.settings
        macauth_settings = settings.getsection("who.plugin.macauth")
        macauth_settings.pop("use", None)
        auth_plugin = SagradaMACAuthPlugin(**macauth_settings)
        req = Request.blank(self.host_url)
        self.auth_token, self.auth_secret = auth_plugin.encode_mac_id(req, {
            "uid": self.user_id
        })

    def _cleanup_test_databases(self):
        # Don't cleanup databases unless we created them ourselves.
        if not self.distant:
            super(StorageFunctionalTestCase, self)._cleanup_test_databases()


def authenticate_to_token_server(url, email=None, audience=None):
    """Authenticate to the given token-server URL.

    This function generates a testing assertion for the specified email
    address, passes it to the specified token-server URL, and returns the
    resulting dict of authentication data.  It's useful for testing things
    that depend on having a live token-server.
    """
    # These modules are not (yet) hard dependencies of syncstorage,
    # so only import them is we really need them.
    import requests
    from browserid.tests.support import make_assertion
    if email is None:
        email = "user_%s@loadtest.local" % (random.randint(1, 100000),)
    if audience is None:
        audience = "https://persona.org"
    assertion = make_assertion(
        email=email,
        audience=audience,
        issuer="loadtest.local",
    )
    r = requests.get(url, headers={
        "Authorization": "Browser-ID " + assertion,
    })
    r.raise_for_status()
    creds = json.loads(r.content)
    for key in ("id", "key", "api_endpoint"):
        creds[key] = creds[key].encode("ascii")
    return creds


def run_live_functional_tests(TestCaseClass, argv=None):
    """Execute the given suite of testcases against a live server."""
    if argv is None:
        argv = sys.argv

    # This will only work using a StorageFunctionalTestCase subclass,
    # since we override the _authenticate() method.
    assert issubclass(TestCaseClass, StorageFunctionalTestCase)

    usage = "Usage: %prog [options] <server-url>"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-x", "--failfast", action="store_true",
                      help="stop after the first failed test")
    parser.add_option("", "--config-file",
                      help="name of the config file in use by the server")
    parser.add_option("", "--use-token-server", action="store_true",
                      help="the given URL is a tokenserver, not an endpoint")
    parser.add_option("", "--email",
                      help="email address to use for tokenserver tests")
    parser.add_option("", "--audience",
                      help="assertion audience to use for tokenserver tests")

    try:
        opts, args = parser.parse_args(argv)
    except SystemExit, e:
        return e.args[0]
    if len(args) != 2:
        parser.print_usage()
        return 2

    url = args[1]
    if opts.config_file is not None:
        os.environ["MOZSVC_TEST_INI_FILE"] = opts.config_file

    # If we're not using the tokenserver, the default implementation of
    # _authenticate will do just fine.
    if not opts.use_token_server:
        if opts.email is not None:
            msg = "cant specify email address unless using live tokenserver"
            raise ValueError(msg)
        if opts.audience is not None:
            msg = "cant specify audience unless using live tokenserver"
            raise ValueError(msg)
        os.environ["MOZSVC_TEST_REMOTE"] = url
        LiveTestCases = TestCaseClass

    # If we're using a live tokenserver, then we need to get some credentials
    # and an endpoint URL.
    else:
        creds = authenticate_to_token_server(url, opts.email, opts.audience)

        # Point the tests at the given endpoint URI.
        host_url = urlparse.urlparse(creds["api_endpoint"])._replace(path="")
        os.environ["MOZSVC_TEST_REMOTE"] = host_url.geturl()

        # Customize the tests to use the provisioned auth credentials.
        class LiveTestCases(TestCaseClass):
            def _authenticate(self):
                self.user_id = creds["uid"]
                self.auth_token = creds["id"].encode("ascii")
                self.auth_secret = creds["key"].encode("ascii")

    # Now use the unittest2 runner to execute them.
    suite = unittest2.TestSuite()
    suite.addTest(unittest2.makeSuite(LiveTestCases))
    runner = unittest2.TextTestRunner(
        stream=sys.stderr,
        failfast=opts.failfast,
    )
    res = runner.run(suite)
    if not res.wasSuccessful():
        return 1
    return 0


# Tell over-zealous test discovery frameworks that this isn't a real test.
run_live_functional_tests.__test__ = False
