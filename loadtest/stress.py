# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Load test for the Storage server
"""
import os
import base64
import random
import json
import time

from funkload.FunkLoadTestCase import FunkLoadTestCase
from funkload.utils import Data

HOST = 'http://localhost:5000'
VERSION = '1.1'
collections = ['bookmarks', 'forms', 'passwords', 'history', 'prefs', 'tabs']


class StressTest(FunkLoadTestCase):

    def setUp(self):
        self.root = 'http://localhost'

    def test_get_post(self):
        username = "ziafhkdmaey2s6v5beidpzenllgj6276"
        password = "passat76"
        self.setBasicAuth(username, password)

        # GET /username/info/collections
        url = "/%s/%s/info/collections" % (VERSION, username)
        url = self.root + url
        response = self.get(url)
        self.assertEqual(response.code, 200)

        # GET /username/storage/collection
        collection = random.choice(collections)
        url = "/%s/%s/storage/%s?full=1" % (VERSION, username, collection)
        url = self.root + url
        response = self.get(url)
        self.assertEqual(response.code, 200)
        body = response.body
        self.assertTrue(body != '')

        # POST /username/storage/collection
        collection = random.choice(collections)
        payload = 'a' * 1000
        url = "/%s/%s/storage/%s" % (VERSION, username, collection)
        url = self.root + url

        # we want to put a batch of 100, 4 times
        # like FF does
        for x in range(4):
            data = []
            for i in range(100):
                id = base64.b64encode(os.urandom(10))
                id += str(time.time() % 100)
                wbo = {'id': id, 'payload': payload}
                data.append(wbo)
            data = json.dumps(data)
            data = Data('application/json', data)
            response = self.post(url, params=data)
            self.assertEqual(response.code, 200)
            body = response.body
            self.assertTrue(body != '')
