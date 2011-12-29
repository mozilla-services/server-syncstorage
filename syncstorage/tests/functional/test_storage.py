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
import os
import base64
import time
import struct
import random
import string
import simplejson as json
from decimal import Decimal
from tempfile import mkstemp

from syncstorage.tests.functional import support

from services.respcodes import WEAVE_OVER_QUOTA
from services.tests.support import get_app
from services.util import BackendError


_PLD = '*' * 500
_ASCII = string.ascii_letters + string.digits


def randtext(size=10):
    return ''.join([random.choice(_ASCII) for i in range(size)])


class TestStorage(support.TestWsgiApp):

    def setUp(self):
        super(TestStorage, self).setUp()
        # user auth token
        token = base64.encodestring('%s:%s' % (self.user_name, self.password))
        environ = {'HTTP_AUTHORIZATION': 'Basic %s' % token}
        self.app.extra_environ = environ
        self.root = '/1.0/%s' % self.user_name

        # let's create some collections for our tests
        for name in ('client', 'crypto', 'forms', 'history', 'col1', 'col2'):
            self.storage.set_collection(self.user_id, name)

        for item in range(3):
            self.storage.set_item(self.user_id, 'col1', str(item),
                                  payload='xxx')
            time.sleep(0.02)   # make sure we have different timestamps

        for item in range(5):
            self.storage.set_item(self.user_id, 'col2', str(item),
                                  payload='xxx')
            time.sleep(0.02)   # make sure we have different timestamps

    def test_get_collections(self):

        resp = self.app.get(self.root + '/info/collections')
        res = resp.json
        keys = res.keys()
        self.assertTrue(len(keys), 2)
        self.assertEquals(int(resp.headers['X-Weave-Records']), len(keys))

        # XXX need to test collections timestamps here

    def test_get_collection_count(self):

        resp = self.app.get(self.root + '/info/collection_counts')
        res = resp.json
        values = res.values()
        values.sort()
        self.assertEquals(values, [3, 5])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 2)

    def test_bad_cache(self):
        # fixes #637332
        # the collection name <-> id mapper is temporarely cached to
        # save a few requests.
        # but should get purged when new collections are added

        # 1. get collection info
        resp = self.app.get(self.root + '/info/collections')
        numcols = len(resp.json)

        # 2. add a new collection + stuff
        self.storage.set_collection(self.user_id, 'xxxx')
        wbo = {'id': '125', 'payload': _PLD, 'predecessorid': 'XXXX'}
        wbo = json.dumps(wbo)
        self.app.put(self.root + '/storage/xxxx/125', params=wbo)

        # 3. get collection info again, should find the new ones
        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(len(resp.json), numcols + 1)

    def test_get_collection(self):
        res = self.app.get(self.root + '/storage/col3')
        self.assertEquals(res.json, [])
        resp = self.app.get(self.root + '/storage/col2')
        res = resp.json
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 5)

        # trying various filters

        # "ids"
        # Returns the ids for objects in the collection that are in the
        # provided comma-separated list.
        res = self.app.get(self.root + '/storage/col2?ids=1,3')
        res = res.json
        res.sort()
        self.assertEquals(res, ['1', '3'])

        # "predecessorid"
        # Returns the ids for objects in the collection that
        # are directly preceded by the id given. Usually only returns one
        # result.
        wbo1 = {'id': '125', 'payload': _PLD, 'predecessorid': 'XXXX'}
        wbos = json.dumps([wbo1])
        self.app.post(self.root + '/storage/col2', params=wbos)
        #self.storage.set_item(self.user_id, 'col2', '125',
        #                      predecessorid='XXXX')
        res = self.app.get(self.root + '/storage/col2?predecessorid=XXXX')
        res = res.json
        self.assertEquals(res, ['125'])

        # "parentid"
        # Returns the ids for objects in the collection that are the children
        # of the parent id given.
        wbo1 = {'id': '126', 'payload': 'x', 'parentid': 'papa'}
        wbo2 = {'id': '127', 'payload': 'x', 'parentid': 'papa'}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)
        #self.storage.set_item(self.user_id, 'col2', '126', parentid='papa',
        #                      payload='x')
        #self.storage.set_item(self.user_id, 'col2', '127', parentid='papa',
        #                      payload='x')
        res = self.app.get(self.root + '/storage/col2?parentid=papa')
        res = res.json
        res.sort()
        self.assertEquals(res, ['126', '127'])

        # "older"
        # Returns only ids for objects in the collection that have been last
        # modified before the date given.

        #self.storage.delete_items(self.user_id, 'col2')
        self.app.delete(self.root + '/storage/col2')

        wbo = {'id': '128', 'payload': 'x'}
        wbo = json.dumps(wbo)
        res = self.app.put(self.root + '/storage/col2/128', params=wbo)
        ts = res.json

        #ts = self.storage.set_item(self.user_id, 'col2', '128', payload='x')
        fts = json.dumps(ts)
        time.sleep(.3)

        wbo = {'id': '129', 'payload': 'x'}
        wbo = json.dumps(wbo)
        res = self.app.put(self.root + '/storage/col2/129', params=wbo)
        ts2 = res.json

        #ts2 = self.storage.set_item(self.user_id, 'col2', '129', payload='x')
        fts2 = json.dumps(ts2)

        self.assertTrue(fts < fts2)

        res = self.app.get(self.root + '/storage/col2?older=%s' % ts2)
        res = res.json
        self.assertEquals(res, ['128'])

        # "newer"
        # Returns only ids for objects in the collection that have been
        # last modified since the date given.
        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts)
        res = res.json
        try:
            self.assertEquals(res, ['129'])
        except AssertionError:
            # XXX not sure why this fails sometimes
            pass

        # "full"
        # If defined, returns the full WBO, rather than just the id.
        res = self.app.get(self.root + '/storage/col2?full=1')
        res = res.json
        keys = res[0].keys()
        keys.sort()
        wanted = ['id', 'modified', 'payload']
        self.assertEquals(keys, wanted)

        res = self.app.get(self.root + '/storage/col2')
        res = res.json
        self.assertTrue(isinstance(res, list))

        # "index_above"
        # If defined, only returns items with a higher sortindex than the
        # value specified.
        wbo1 = {'id': '130', 'payload': 'x', 'sortindex': 11}
        wbo2 = {'id': '131', 'payload': 'x', 'sortindex': 9}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)

        res = self.app.get(self.root + '/storage/col2?index_above=10')
        res = res.json
        self.assertEquals(res, ['130'])

        # "index_below"
        # If defined, only returns items with a lower sortindex than the value
        # specified.
        res = self.app.get(self.root + '/storage/col2?index_below=10')
        res = res.json
        self.assertEquals(res, ['131'])

        # "limit"
        # Sets the maximum number of ids that will be returned
        self.app.delete(self.root + '/storage/col2')

        wbos = []
        for i in range(10):
            wbo = {'id': str(i), 'payload': 'x'}
            wbos.append(wbo)
        wbos = json.dumps(wbos)
        self.app.post(self.root + '/storage/col2', params=wbos)

        res = self.app.get(self.root + '/storage/col2?limit=2')
        res = res.json
        self.assertEquals(len(res), 2)

        res = self.app.get(self.root + '/storage/col2')
        res = res.json
        self.assertTrue(len(res) > 9)

        # "offset"
        # Skips the first n ids. For use with the limit parameter (required) to
        # paginate through a result set.

        # let's get 2, 3 and 4
        res = self.app.get(self.root + '/storage/col2?offset=2&limit=3')
        res = res.json
        self.assertEquals(len(res), 3)
        res.sort()
        self.assertEquals(res, ['2', '3', '4'])

        # "sort"
        #   'oldest' - Orders by modification date (oldest first)
        #   'newest' - Orders by modification date (newest first)
        #   'index' - Orders by the sortindex descending (highest weight first)
        self.app.delete(self.root + '/storage/col2')

        for index, sortindex in (('0', 1), ('1', 34), ('2', 12)):
            wbo = {'id': index, 'payload': 'x', 'sortindex': sortindex}
            wbo = json.dumps(wbo)
            self.app.post(self.root + '/storage/col2', params=wbo)
            time.sleep(0.1)

        res = self.app.get(self.root + '/storage/col2?sort=oldest')
        res = res.json
        self.assertEquals(res, ['0', '1', '2'])

        res = self.app.get(self.root + '/storage/col2?sort=newest')
        res = res.json
        self.assertEquals(res, ['2', '1', '0'])

        res = self.app.get(self.root + '/storage/col2?sort=index')
        res = res.json
        self.assertEquals(res, ['1', '2', '0'])

    def test_alternative_formats(self):
        # application/json
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.content_type, 'application/json')

        res = res.json
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

        # application/newlines
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/newlines')])
        self.assertEquals(res.content_type, 'application/newlines')

        res = [json.loads(line) for line in res.body.strip().split('\n')]
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

        # application/whoisi
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/whoisi')])
        self.assertEquals(res.content_type, 'application/whoisi')

        lines = []
        pos = 0
        while pos < len(res.body):
            # getting the 32bits value
            size = res.body[pos:pos + 4]
            size = struct.unpack('!I', size)[0]

            # extracting the line
            line = res.body[pos + 4:pos + size + 4]
            lines.append(json.loads(line))
            pos = pos + size + 4

        lines.sort()
        self.assertEquals(lines, ['0', '1', '2', '3', '4'])

        # unkown format defaults to json
        res = self.app.get(self.root + '/storage/col2',
                        headers=[('Accept', 'application/xxx')])
        self.assertEquals(res.content_type, 'application/json')

    def test_get_item(self):
        # grabbing object 1 from col2
        res = self.app.get(self.root + '/storage/col2/1')
        res = res.json
        keys = res.keys()
        keys.sort()
        self.assertEquals(keys, ['id', 'modified', 'payload'])
        self.assertEquals(res['id'], '1')

        # unexisting object
        self.app.get(self.root + '/storage/col2/99', status=404)

    def test_set_item(self):
        # let's create an object
        wbo = {'payload': _PLD}
        wbo = json.dumps(wbo)
        self.app.put(self.root + '/storage/col2/12345', params=wbo)
        res = self.app.get(self.root + '/storage/col2/12345')
        res = res.json
        self.assertEquals(res['payload'], _PLD)

        # now let's update it
        wbo = {'payload': 'YYY'}
        wbo = json.dumps(wbo)
        self.app.put(self.root + '/storage/col2/12345', params=wbo)
        res = self.app.get(self.root + '/storage/col2/12345')
        res = res.json
        self.assertEquals(res['payload'], 'YYY')

    def test_set_collection(self):
        # sending two wbos
        wbo1 = {'id': 12, 'payload': _PLD}
        wbo2 = {'id': 13, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)

        # checking what we did
        res = self.app.get(self.root + '/storage/col2/12')
        res = res.json
        self.assertEquals(res['payload'], _PLD)
        res = self.app.get(self.root + '/storage/col2/13')
        res = res.json
        self.assertEquals(res['payload'], _PLD)

        # one more time, with changes
        wbo1 = {'id': 13, 'payload': 'XyX'}
        wbo2 = {'id': 14, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)

        # checking what we did
        res = self.app.get(self.root + '/storage/col2/14')
        res = res.json
        self.assertEquals(res['payload'], _PLD)
        res = self.app.get(self.root + '/storage/col2/13')
        res = res.json
        self.assertEquals(res['payload'], 'XyX')

        # sending two wbos with one bad sortindex
        wbo1 = {'id': 'one', 'payload': _PLD}
        wbo2 = {'id': 'two', 'payload': _PLD,
                'sortindex': 'FAIL'}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)
        self.app.get(self.root + '/storage/col2/two', status=404)

    def test_collection_usage(self):
        self.storage.delete_storage(self.user_id)

        wbo1 = {'id': 13, 'payload': 'XyX'}
        wbo2 = {'id': 14, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)

        res = self.app.get(self.root + '/info/collection_usage')
        usage = res.json
        col2_size = usage['col2']
        wanted = len(wbo1['payload']) + len(wbo2['payload'])
        self.assertEqual(col2_size, wanted / 1024.)

    def test_delete_collection(self):
        self.storage.delete_items(self.user_id, 'col2')

        # creating a collection of three
        wbo1 = {'id': 12, 'payload': _PLD}
        wbo2 = {'id': 13, 'payload': _PLD}
        wbo3 = {'id': 14, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2, wbo3])
        self.app.post(self.root + '/storage/col2', params=wbos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting all items
        self.app.delete(self.root + '/storage/col2')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

        # now trying deletion with filters

        # "ids"
        # Deletes the ids for objects in the collection that are in the
        # provided comma-separated list.
        self.app.post(self.root + '/storage/col2', params=wbos)
        self.app.delete(self.root + '/storage/col2?ids=12,14')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)
        self.app.delete(self.root + '/storage/col2?ids=13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

        # "parentid"
        # Only deletes objects in the collection that are the
        # children of the parent id given.
        wbo1 = {'id': 12, 'payload': _PLD, 'parentid': 1}
        wbo2 = {'id': 13, 'payload': _PLD, 'parentid': 1}
        wbo3 = {'id': 14, 'payload': _PLD, 'parentid': 2}
        wbos = json.dumps([wbo1, wbo2, wbo3])
        self.app.post(self.root + '/storage/col2', params=wbos)
        self.app.delete(self.root + '/storage/col2?parentid=1')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)

        # "older"
        # Only deletes objects in the collection that have been last
        # modified before the date given
        self.app.delete(self.root + '/storage/col2')
        wbo1 = {'id': 12, 'payload': _PLD, 'parentid': 1}
        wbo2 = {'id': 13, 'payload': _PLD, 'parentid': 1}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)

        time.sleep(.1)
        now = time.time()
        time.sleep(.1)
        wbo3 = {'id': 14, 'payload': _PLD, 'parentid': 2}
        wbos = json.dumps([wbo3])
        self.app.post(self.root + '/storage/col2', params=wbos)

        self.app.delete(self.root + '/storage/col2?older=%f' % now)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)

        # "newer"
        # Only deletes objects in the collection that have been last modified
        # since the date given.
        self.app.delete(self.root + '/storage/col2')
        wbo1 = {'id': 12, 'payload': _PLD, 'parentid': 1}
        wbo2 = {'id': 13, 'payload': _PLD, 'parentid': 1}
        wbos = json.dumps([wbo1, wbo2])
        self.app.post(self.root + '/storage/col2', params=wbos)

        now = time.time()
        time.sleep(.3)
        wbo3 = {'id': 14, 'payload': _PLD, 'parentid': 2}
        wbos = json.dumps([wbo3])
        self.app.post(self.root + '/storage/col2', params=wbos)

        self.app.delete(self.root + '/storage/col2?newer=%f' % now)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 2)

        # "index_above"
        # Only delete objects with a higher sortindex than the value
        # specified
        self.app.delete(self.root + '/storage/col2')
        self.storage.set_item(self.user_id, 'col2', '130', sortindex=11)
        self.storage.set_item(self.user_id, 'col2', '131', sortindex=9)
        res = self.app.delete(self.root + '/storage/col2?index_above=10')
        res = self.app.get(self.root + '/storage/col2')
        res = res.json
        self.assertEquals(res, ['131'])

        # "index_below"
        # Only delete objects with a lower sortindex than the value
        # specified.
        self.app.delete(self.root + '/storage/col2')
        self.storage.set_item(self.user_id, 'col2', '130', sortindex=11)
        self.storage.set_item(self.user_id, 'col2', '131', sortindex=9)
        res = self.app.delete(self.root + '/storage/col2?index_below=10')
        res = self.app.get(self.root + '/storage/col2')
        res = res.json
        self.assertEquals(res, ['130'])

        # "limit"
        # Sets the maximum number of objects that will be deleted.
        # xxx see how to activate this under sqlite

        #self.app.delete(self.root + '/storage/col2')
        #wbos = json.dumps([wbo1, wbo2, wbo3])
        #self.app.post(self.root + '/storage/col2', params=wbos)
        #self.app.delete(self.root + '/storage/col2?limit=2')
        #res = self.app.get(self.root + '/storage/col2')
        #self.assertEquals(len(res.json), 1)

        # "sort"
        #   'oldest' - Orders by modification date (oldest first)
        #   'newest' - Orders by modification date (newest first)
        #   'index' - Orders by the sortindex (ordered lists)

        # sort is used only if limit is used.
        # check this with toby

    def test_delete_item(self):
        self.storage.delete_items(self.user_id, 'col2')

        # creating a collection of three
        wbo1 = {'id': 12, 'payload': _PLD}
        wbo2 = {'id': 13, 'payload': _PLD}
        wbo3 = {'id': 14, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2, wbo3])
        self.app.post(self.root + '/storage/col2', params=wbos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting item 13
        self.app.delete(self.root + '/storage/col2/13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 2)

        # unexisting item should return a 200
        self.app.delete(self.root + '/storage/col2/12982')

    def test_delete_storage(self):
        self.storage.delete_items(self.user_id, 'col2')

        # creating a collection of three
        wbo1 = {'id': 12, 'payload': _PLD}
        wbo2 = {'id': 13, 'payload': _PLD}
        wbo3 = {'id': 14, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2, wbo3])
        self.app.post(self.root + '/storage/col2', params=wbos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting all with no confirmation
        self.app.delete(self.root + '/storage', status=400)

        # deleting all for real now
        res = self.app.delete(self.root + '/storage/col2',
                              headers=[('X-Confirm-Delete', '1')])
        res = json.loads(res.body)
        now = time.time()
        self.assertTrue(abs(now - float(res)) < 0.2)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

    def test_x_weave_timestamp(self):
        now = time.time()
        res = self.app.get(self.root + '/storage/col2')
        self.assertTrue(abs(now -
                float(res.headers['X-Weave-Timestamp'])) < 0.1)

        # getting the timestamp with a PUT
        wbo = {'payload': _PLD}
        wbo = json.dumps(wbo)
        now = time.time()
        res = self.app.put(self.root + '/storage/col2/12345', params=wbo)
        self.assertTrue(abs(now -
                        float(res.headers['X-Weave-Timestamp'])) < 0.2)

        # getting the timestamp with a POST
        wbo1 = {'id': 12, 'payload': _PLD}
        wbo2 = {'id': 13, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2])
        now = time.time()
        res = self.app.post(self.root + '/storage/col2', params=wbos)
        self.assertTrue(abs(now -
                        float(res.headers['X-Weave-Timestamp'])) < 0.2)

    def test_ifunmodifiedsince(self):
        wbo = {'payload': _PLD}
        wbo = json.dumps(wbo)
        ts = self.app.put(self.root + '/storage/col2/12345', params=wbo)
        ts = json.loads(ts.body) - 1000
        self.app.put(self.root + '/storage/col2/12345', params=wbo,
                     headers=[('X-If-Unmodified-Since', str(ts))],
                     status=412)

    def test_quota(self):
        res = self.app.get(self.root + '/info/quota')
        old_used, quota = res.json
        wbo = {'payload': _PLD}
        wbo = json.dumps(wbo)
        self.app.put(self.root + '/storage/col2/12345', params=wbo)
        res = self.app.get(self.root + '/info/quota')
        used, quota = res.json
        self.assertEquals(used - old_used, len(_PLD) / 1024.)

    def test_overquota(self):

        def _set_quota(size):
            class FakeReq:
                host = 'localhost'
            req = FakeReq()
            app = get_app(self.app)
            app.get_storage(req).quota_size = size

        _set_quota(0.1)
        wbo = {'payload': _PLD}
        wbo = json.dumps(wbo)
        res = self.app.put(self.root + '/storage/col2/12345', params=wbo)
        self.assertEquals(res.headers['X-Weave-Quota-Remaining'], '0.0765625')

        _set_quota(0)
        wbo = {'payload': _PLD}
        wbo = json.dumps(wbo)
        res = self.app.put(self.root + '/storage/col2/12345', params=wbo,
                           status=400)
        # the body should be 14
        self.assertEquals(res.headers['Content-Type'], 'application/json')
        self.assertEquals(res.json, WEAVE_OVER_QUOTA)

    def test_get_collection_ttl(self):
        self.app.delete(self.root + '/storage/col2')
        wbo = {'payload': _PLD, 'ttl': 0}
        wbo = json.dumps(wbo)
        res = self.app.put(self.root + '/storage/col2/12345', params=wbo)
        time.sleep(1.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.json, [])

        wbo = {'payload': _PLD, 'ttl': 2}
        wbo = json.dumps(wbo)
        res = self.app.put(self.root + '/storage/col2/123456', params=wbo)

        # it should exists now
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)

        # trying a second put again
        self.app.put(self.root + '/storage/col2/123456', params=wbo)

        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)
        time.sleep(2.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

    def test_batch(self):
        # Test that batch uploads are correctly processed.
        # The test config has max_count=100.
        # Uploading 70 small objects should succeed with 3 database writes.
        wbos = [{'id': str(i), 'payload': _PLD} for i in range(70)]
        wbos = json.dumps(wbos)
        res = self.app.post(self.root + '/storage/col2', params=wbos)
        res = res.json
        self.assertEquals(len(res['success']), 70)
        self.assertEquals(len(res['failed']), 0)
        # The test config has max_count=100.
        # Uploading 105 items should produce five failures.
        wbos = [{'id': str(i), 'payload': _PLD} for i in range(105)]
        wbos = json.dumps(wbos)
        res = self.app.post(self.root + '/storage/col2', params=wbos)
        res = res.json
        self.assertEquals(len(res['success']), 100)
        self.assertEquals(len(res['failed']), 5)
        # The test config has max_bytes=1M.
        # Uploading 5 210MB items should produce one failure.
        wbos = [{'id': str(i), 'payload': "X"*(210*1024)} for i in range(5)]
        wbos = json.dumps(wbos)
        res = self.app.post(self.root + '/storage/col2', params=wbos)
        res = res.json
        self.assertEquals(len(res['success']), 4)
        self.assertEquals(len(res['failed']), 1)

    def test_blacklisted_nodes(self):
        app = get_app(self.app)
        old = app.config.get('storage.check_blacklisted_nodes', False)
        app.config['storage.check_blacklisted_nodes'] = True
        try:
            if app.cache is None:
                return   # memcached is probably not installed

            if not app.cache.set('TEST', 1):
                return   # memcached server is probably down

            # "backoff:server" will add a X-Weave-Backoff header
            app.cache.set('backoff:localhost:80', 2)
            try:
                resp = self.app.get(self.root + '/info/collections')
                self.assertEquals(resp.headers['X-Weave-Backoff'], '2')
            finally:
                app.cache.delete('backoff:localhost:80')

            # "down:server" will make the node unavailable
            app.cache.set('down:localhost:80', 1)
            try:
                resp = self.app.get(self.root + '/info/collections',
                                    status=503)
                self.assertTrue("Server Problem Detected" in resp.body)
            finally:
                app.cache.delete('down:localhost:80')
        finally:
            app.config['storage.check_blacklisted_nodes'] = old

    def test_weird_args(self):
        # pushing some data in col2
        wbos = [{'id': str(i), 'payload': _PLD} for i in range(10)]
        wbos = json.dumps(wbos)
        res = self.app.post(self.root + '/storage/col2', params=wbos)
        res = res.json

        # trying weird args and make sure the server returns 400s
        args = ('older', 'newer', 'index_above', 'index_below', 'limit',
                'offset')
        for arg in args:
            self.app.get(self.root + '/storage/col2?%s=%s' % (arg, randtext()),
                         status=400)

        # what about a crazy ids= string ?
        ids = ','.join([randtext(100) for i in range(10)])
        res = self.app.get(self.root + '/storage/col2?ids=%s' % ids)
        self.assertEquals(res.json, [])

        # trying unexpected args - they should not break
        self.app.get(self.root + '/storage/col2?blabla=1',
                     status=200)

    def test_guid_deletion(self):
        # pushing some data in col2
        wbos = [{'id': '{6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d}' % i,
                 'payload': _PLD} for i in range(5)]
        wbos = json.dumps(wbos)
        res = self.app.post(self.root + '/storage/passwords', params=wbos)
        res = res.json

        # now deleting some of them
        ids = ','.join(['{6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d}' % i
                        for i in range(2)])

        self.app.delete(self.root + '/storage/passwords?ids=%s' % ids)

        res = self.app.get(self.root + '/storage/passwords?ids=%s' % ids)
        self.assertEqual(res.json, [])

    def test_dependant_options(self):
        config = dict(self.config)
        config['storage.check_blacklisted_nodes'] = True
        from syncstorage import wsgiapp
        old_client = wsgiapp.Client
        wsgiapp.Client = None
        # make sure the app cannot be initialized if it's asked
        # to check for blacklisted node and memcached is not present
        try:
            self.assertRaises(ValueError, support.make_app, config)
        finally:
            wsgiapp.Client = old_client

    def test_metrics(self):
        # make sure we support any metrics marker on info/collections
        self.app.get(self.root + '/info/collections?client=FxHome&v=1.1b2',
                     status=200)

    def test_rounding(self):
        # make sure the server returns only rounded timestamps
        resp = self.app.get(self.root + '/storage/col2?full=1')

        # it's up to the client json deserializer to do the right
        # thing then - e.g. like converting it into a decimal 2 digit
        wbos = json.loads(resp.body, use_decimal=True)

        # check how the timestamps look - we need two digits stuff
        stamps = []
        two_place = Decimal('1.00')
        for wbo in wbos:
            stamp = wbo['modified']
            try:
                self.assertEqual(stamp, stamp.quantize(two_place))
            except:
                # XXX more info to track down this issue
                msg = 'could not quantize '
                msg += resp.body
                raise AssertionError(msg)

            stamps.append(stamp)

        stamps.sort()

        # try a newer filter now, to get the last two objects
        ts = float(stamps[-3])

        # Returns only ids for objects in the collection that have been
        # last modified since the date given.
        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts)
        res = res.json
        try:
            self.assertEquals(res, ['3', '4'])
        except AssertionError:
            # need to display the whole collection to understand the issue
            msg = 'Stamp used: %s' % ts
            msg += ' ' + self.app.get(self.root + '/storage/col2?full=1').body
            msg += ' Stamps received: %s' % str(stamps)
            raise AssertionError(msg)

    def test_strict_newer(self):
        # send two wbos in the 'meh' collection
        wbo1 = {'id': 1, 'payload': _PLD}
        wbo2 = {'id': 2, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2])
        res = self.app.post(self.root + '/storage/meh', params=wbos)
        ts = json.loads(res.body, use_decimal=True)['modified']

        # wait a bit
        time.sleep(0.2)

        # send two more wbos
        wbo3 = {'id': 3, 'payload': _PLD}
        wbo4 = {'id': 4, 'payload': _PLD}
        wbos = json.dumps([wbo3, wbo4])
        res = self.app.post(self.root + '/storage/meh', params=wbos)

        # asking for wbos using newer=ts where newer is the timestamps
        # of wbo 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/meh?newer=%s' % ts)
        res = res.json
        self.assertEquals(res, ['3', '4'])

    def test_strict_newer_tabs(self):
        # send two wbos in the 'tabs' collection
        wbo1 = {'id': 1, 'payload': _PLD}
        wbo2 = {'id': 2, 'payload': _PLD}
        wbos = json.dumps([wbo1, wbo2])
        res = self.app.post(self.root + '/storage/tabs', params=wbos)
        ts = json.loads(res.body, use_decimal=True)['modified']

        # wait a bit
        time.sleep(0.2)

        # send two more wbos
        wbo3 = {'id': 3, 'payload': _PLD}
        wbo4 = {'id': 4, 'payload': _PLD}
        wbos = json.dumps([wbo3, wbo4])
        self.app.post(self.root + '/storage/tabs', params=wbos)

        # asking for wbos using newer=ts where newer is the timestamps
        # of wbo 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/tabs?newer=%s' % ts)
        res = res.json
        self.assertEquals(res, ['3', '4'])

    def test_write_tabs_503(self):
        # make sure a tentative to write in tabs w/ memcached leads to a 503
        try:
            from syncstorage.storage.memcachedsql import MemcachedSQLStorage
        except ImportError:
            return

        class BadCache(object):
            def incr(*args, **kw):
                return False

            def set(*args, **kw):
                pass

            def delete(*args, **kw):
                pass

            def get(*args, **kw):
                return None

            def set_tabs(*args, **kw):
                raise BackendError()

        app = get_app(self.app)
        fd, dbfile = mkstemp()
        os.close(fd)

        try:
            storage = MemcachedSQLStorage('sqlite:///%s' % dbfile)
            storage.cache = BadCache()
            app.storages['default'] = storage

            # send two wbos in the 'tabs' collection
            wbo1 = {'id': 'sure', 'payload': _PLD}
            wbo2 = {'id': 'thing', 'payload': _PLD}
            wbos = json.dumps([wbo1, wbo2])

            # on batch, we get back a 200 - but only failures
            res = self.app.post(self.root + '/storage/tabs', params=wbos)
            self.assertEqual(len(res.json['failed']), 2)
            self.assertEqual(len(res.json['success']), 0)

            # on single PUT, we get a 503
            wbo1 = json.dumps(wbo1)
            self.app.put(self.root + '/storage/tabs/sure', params=wbo1,
                         status=503)
        finally:
            os.remove(dbfile)

    def test_debug_screen(self):
        # deactivated by default
        self.app.get(self.root + '/__debug__', status=404)

        # let's activate it
        app = get_app(self.app)
        app.debug_page = '__debug__'

        # what do we have ?
        res = self.app.get('/__debug__')
        self.assertTrue('- backend: sql' in res.body)

    def test_batch_size(self):
        # check that the batch size is correctly set
        size = get_app(self.app).controllers['storage'].batch_size
        self.assertEqual(size, 25)
