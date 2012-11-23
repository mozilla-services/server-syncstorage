# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Load test for the SyncStorage server
"""

import os
import base64
import random
import json
import time
from ConfigParser import NoOptionError

from funkload.FunkLoadTestCase import FunkLoadTestCase
from funkload.utils import Data

import macauthlib
from webob import Request
from mozsvc.user import SagradaAuthenticationPolicy

from syncstorage.tests.functional.support import authenticate_to_token_server


# The collections to operate on.
# Each operation will randomly select a collection from this list.
# The "tabs" collection is not included since it uses memcache; we need
# to figure out a way to test it without overloading the server.
collections = ['bookmarks', 'forms', 'passwords', 'history', 'prefs']

# The distribution of GET operations to meta/global per test run.
# 40% will do 0 GETs, 60% will do 1 GET, etc...
metaglobal_count_distribution = [40, 60, 0, 0, 0]

# The distribution of GET operations per test run.
# 71% will do 0 GETs, 15% will do 1 GET, etc...
get_count_distribution = [71, 15, 7, 4, 3]

# The distribution of POST operations per test run.
# 67% will do 0 POSTs, 18% will do 1 POST, etc...
post_count_distribution = [67, 18, 9, 4, 2]

# The distribution of DELETE operations per test run.
# 99% will do 0 DELETEs, 1% will do 1 DELETE, etc...
delete_count_distribution = [99, 1, 0, 0, 0]

# The probability that we'll try to do a full DELETE of all data.
# Expressed as a float between 0 and 1.
deleteall_probability = 1 / 100.


class StressTest(FunkLoadTestCase):

    # Customize the _browse() method to send authentication headers on all
    # requests, and to perform additional logging.

    def _browse(self, url_in, params_in=None, description=None, ok_codes=None,
                method='post', *args, **kwds):
        # Automatically add a fresh MACAuth header to the request.
        if self.auth_token is not None:
            req = Request.blank(url_in)
            req.method = method.upper()
            if params_in and req.method in ("GET", "HEAD",):
                qs_items = ("%s=%s" % item for item in params_in.iteritems())
                req.query_string = "&".join(qs_items)
            macauthlib.sign_request(req, self.auth_token, self.auth_secret)
            self.setHeader("Authorization", req.environ["HTTP_AUTHORIZATION"])
        # Log the start and end of the request, for debugging purposes.
        args = (url_in, params_in, description, ok_codes, method) + args
        self.logi("%s: %s" % (method.upper(), url_in))
        try:
            result = super(StressTest, self)._browse(*args, **kwds)
        except Exception, e:
            self.logi("    FAIL: " + url_in + " " + repr(e))
            raise
        else:
            self.logi("    OK: " + url_in + " " + repr(result))
            return result

    def setUp(self):
        # Should we use a tokenserver or synthesize our own?
        try:
            self.token_server_url = self.conf_get("main", "token_server_url")
            self.logi("using tokenserver at %s" % (self.token_server_url,))
        except NoOptionError:
            self.token_server_url = None
            secrets_file = self.conf_get("main", "secrets_file")
            self.logi("using secrets_file from %s" % (secrets_file,))
            policy = SagradaAuthenticationPolicy(secrets_file=secrets_file)
            self.auth_policy = policy
            nodes = self.conf_get("main", "endpoint_nodes").split("\n")
            nodes = [node.strip() for node in nodes]
            nodes = [node for node in nodes if not node.startswith("#")]
            self.endpoint_nodes = nodes

    def test_storage_session(self):
        self._generate_token_credentials()
        self.logi("using endpoint url %s" % (self.endpoint_url,))

        # Always GET info/collections
        self.setOkCodes([200, 404])
        url = self.endpoint_url + "/info/collections"
        response = self.get(url)

        # GET requests to meta/global.
        num_requests = self._pick_weighted_count(metaglobal_count_distribution)
        self.setOkCodes([200, 404])
        for x in range(num_requests):
            url = self.endpoint_url + "/storage/meta/global"
            response = self.get(url)
            if response.code == 404:
                metapayload = "This is the metaglobal payload which contains"\
                              " some client data that doesnt look much"\
                              " like this"
                data = json.dumps({"id": "global", "payload": metapayload})
                data = Data('application/json', data)
                self.setOkCodes([200, 201])
                self.put(url, params=data)

        # GET requests to individual collections.
        num_requests = self._pick_weighted_count(get_count_distribution)
        cols = random.sample(collections, num_requests)
        self.setOkCodes([200, 404])
        for x in range(num_requests):
            url = self.endpoint_url + "/storage/" + cols[x]
            newer = int(time.time() - random.randint(3600, 360000))
            params = {"full": "1", "newer": str(newer)}
            self.logi("about to GET (x=%d) %s" % (x, url))
            response = self.get(url, params)

        # PUT requests with 100 WBOs batched together
        num_requests = self._pick_weighted_count(post_count_distribution)
        cols = random.sample(collections, num_requests)
        self.setOkCodes([200])
        for x in range(num_requests):
            url = self.endpoint_url + "/storage/" + cols[x]
            data = []
            items_per_batch = 10
            for i in range(items_per_batch):
                id = base64.urlsafe_b64encode(os.urandom(10)).rstrip("=")
                id += str(int((time.time() % 100) * 100000))
                payload = self.auth_token * random.randint(50, 200)
                wbo = {'id': id, 'payload': payload}
                data.append(wbo)
            data = json.dumps(data)
            data = Data('application/json', data)
            self.logi("about to POST (x=%d) %s" % (x, url))
            response = self.post(url, params=data)
            body = response.body
            self.assertTrue(body != '')
            result = json.loads(body)
            self.assertEquals(len(result["success"]), items_per_batch)
            self.assertEquals(len(result["failed"]), 0)

        # DELETE requests.
        # We might choose to delete some individual collections, or to do
        # a full reset and delete all the data.  Never both in the same run.
        num_requests = self._pick_weighted_count(delete_count_distribution)
        self.setOkCodes([204])
        if num_requests:
            cols = random.sample(collections, num_requests)
            for x in range(num_requests):
                url = self.endpoint_url + "/storage/" + cols[x]
                self.delete(url)
        else:
            if random.random() <= deleteall_probability:
                url = self.endpoint_url + "/storage"
                self.delete(url)

    def _generate_token_credentials(self):
        """Pick an identity, log in and generate the auth token."""
        uid = random.randint(1, 1000000)  # existing user
        # Use the tokenserver if configured, otherwise fake it ourselves.
        if self.token_server_url is None:
            self.logi("synthesizing token for uid %s" % (uid,))
            endpoint_node = random.choice(self.endpoint_nodes)
            req = Request.blank(endpoint_node)
            creds = self.auth_policy.encode_mac_id(req, uid)
            self.auth_token, self.auth_secret = creds
            self.endpoint_url = endpoint_node + "/2.0/%s" % (uid,)
        else:
            email = "user%s@mockmyid.com" % (uid,)
            token_url = self.token_server_url + "/1.0/sync/2.0"
            self.logi("requesting token for %s from %s" % (email, token_url))
            credentials = authenticate_to_token_server(token_url, email,
                                               audience=self.token_server_url)
            self.auth_token = credentials["id"]
            self.auth_secret = credentials["key"]
            self.endpoint_url = credentials["api_endpoint"]

        self.logi("assigned endpoint_url %s" % (self.endpoint_url))
        return self.auth_token, self.auth_secret, self.endpoint_url

    def _pick_weighted_count(self, weights):
        i = random.randint(1, sum(weights))
        count = 0
        base = 0
        for weight in weights:
            base += weight
            if i <= base:
                break
            count += 1

        return count
