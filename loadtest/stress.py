# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Sync Server
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2010
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Tarek Ziade (tarek@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
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
