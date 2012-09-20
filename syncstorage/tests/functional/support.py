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
import requests

import macauthlib
import browserid.tests.support

from pyramid.request import Request
from pyramid.interfaces import IAuthenticationPolicy

from mozsvc.tests.support import FunctionalTestCase

from syncstorage.tests.support import StorageTestCase


class StorageFunctionalTestCase(FunctionalTestCase, StorageTestCase):
    """Abstract base class for functional testing of a storage API."""

    def setUp(self):
        super(StorageFunctionalTestCase, self).setUp()

        # Generate userid and auth token crednentials.
        # This can be overridden by subclasses.
        self.config.commit()
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
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        req = Request.blank(self.host_url)
        creds = auth_policy.encode_mac_id(req, self.user_id)
        self.auth_token, self.auth_secret = creds

    def _cleanup_test_databases(self):
        # Don't cleanup databases unless we created them ourselves.
        if not self.distant:
            super(StorageFunctionalTestCase, self)._cleanup_test_databases()


MOCKMYID_PRIVATE_KEY = None
MOCKMYID_PRIVATE_KEY_DATA = {
  "algorithnm": "RS",
  "n": "154988747580902760394650941058372315672655463739759604809411226511"
       "077728241215274831074023538998462524898370248701917073947431963995"
       "829594255139047629967566720896935410098920308488250796497830860055"
       "544424902329008757928517862039480884579424169789764552974280774608"
       "906504095492421246555369861413637195898821600814807850489656862851"
       "420023207670666748797372380120641566758995125031432254819338645077"
       "931184578057920644455028341623155321139637468017701876856504085604"
       "246826549377447138137738969622637096927246306509521595969513482640"
       "050043750176104418359560732757087402395180114009919728116694933566"
       "82993446554779893834303",
  "e": "65537",
  "d": "65399069618723544500872440362363672698042543818900958411270855515"
       "77495913426869112377010004955160417265879626558436936025363204803"
       "91331858268095155890431830889373003315817865054997037936791585608"
       "73644285308283967959957813646594134677848534354507623921570269626"
       "94408807947047846891301466649598749901605789115278274397848888140"
       "10530606360821777612754992672154421572087230519464512940305680198"
       "74227941147032559892027555115234340986250008269684300770919843514"
       "10839837395828971692109391386427709263149504336916566097901771762"
       "64809088099477332528320749664563079224800780517787353244131447050"
       "2254528486411726581424522838833"
}


def authenticate_to_token_server(url, email=None, audience=None):
    """Authenticate to the given token-server URL.

    This function generates a testing assertion for the specified email
    address, passes it to the specified token-server URL, and returns the
    resulting dict of authentication data.  It's useful for testing things
    that depend on having a live token-server.
    """
    # These modules are not (yet) hard dependencies of syncstorage,
    # so only import them is we really need them.
    global MOCKMYID_PRIVATE_KEY
    if MOCKMYID_PRIVATE_KEY is None:
        from browserid.jwt import RS256Key
        MOCKMYID_PRIVATE_KEY = RS256Key(MOCKMYID_PRIVATE_KEY_DATA)
    if email is None:
        email = "user_%s@mockmyid.com" % (random.randint(1, 100000),)
    if audience is None:
        audience = "https://persona.org"
    assertion = browserid.tests.support.make_assertion(
        email=email,
        audience=audience,
        issuer="mockmyid.com",
        issuer_keypair=(None, MOCKMYID_PRIVATE_KEY),
    )
    r = requests.get(url, headers={
        "Authorization": "Browser-ID " + assertion,
        "X-Conditions-Accepted": "true",
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

        # Point the tests at the given endpoint URI, after stripping off
        # the trailing /2.0/UID component.
        host_url = urlparse.urlparse(creds["api_endpoint"])
        host_path = host_url.path.rstrip("/")
        host_path = "/".join(host_path.split("/")[:-2])
        host_url = host_url._replace(path=host_path)
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
