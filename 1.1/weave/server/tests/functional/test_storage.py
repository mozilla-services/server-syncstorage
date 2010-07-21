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
Basic tests to verify that the dispatching mechanism works.
"""
import base64
import json

from weave.server.tests.functional import support


class TestStorage(support.TestWsgiApp):

    def setUp(self):
        super(TestStorage, self).setUp()
        # user auth token
        environ = {'Authorization': 'Basic %s' % \
                        base64.encodestring('tarek:tarek')}
        self.app.extra_environ = environ

        # let's create some collections for our tests
        self.storage.set_user(1)

        for name in ('client', 'crypto', 'forms', 'history', 'col1', 'col2'):
            self.storage.set_collection(1, name)

        for item in range(3):
            self.storage.set_item(1, 'col1', item)
        for item in range(5):
            self.storage.set_item(1, 'col2', item)

    def tearDown(self):
        # removing all data after the test
        self.storage.delete_user(1)
        super(TestStorage, self).tearDown()

    def test_get_collections_info(self):

        res = self.app.get('/1.0/tarek/info/collections')
        self.assertEquals(res.status, '200 OK')
        res = json.loads(res.body)
        keys = res.keys()
        keys.sort()
        # standard collections + custom ones
        wanted = ['client', 'col1', 'col2', 'crypto', 'forms', 'history']
        self.assertEquals(keys, wanted)

        # XXX need to test collections timestamps here

    def test_get_collections_count(self):

        res = self.app.get('/1.0/tarek/info/collection_counts')
        self.assertEquals(res.status, '200 OK')
        res = json.loads(res.body)
        self.assertEquals(res['col1'], 3)
        self.assertEquals(res['col2'], 5)

    def test_get_quota(self):

        # XXX implement the new quota code
        res = self.app.get('/1.0/tarek/info/quota', status=501)
        self.assertEquals(res.status, '501 Not Implemented')

    def test_get_collection(self):
        res = self.app.get('/1.0/tarek/storage/col3')
        self.assertEquals(json.loads(res.body), [])

        res = self.app.get('/1.0/tarek/storage/col2')
        res = json.loads(res.body)
        ids = [line['id'] for line in res]
        ids.sort()
        self.assertEquals(ids, [0, 1, 2, 3, 4])

        # trying various filters

        # "ids"
        # Returns the ids for objects in the collection that are in the
        # provided comma-separated list.
        res = self.app.get('/1.0/tarek/storage/col2?ids=1,3')
        res = json.loads(res.body)
        ids = [line['id'] for line in res]
        ids.sort()
        self.assertEquals(ids, [1, 3])

        # "predecessorid"
        # Returns the ids for objects in the collection that
        # are directly preceded by the id given. Usually only returns one
        # result.
        self.storage.set_item(1, 'col2', 125, predecessorid='XXXX')
        res = self.app.get('/1.0/tarek/storage/col2?predecessorid=XXXX')
        res = json.loads(res.body)
        ids = [line['id'] for line in res]
        ids.sort()
        self.assertEquals(ids, [125])
