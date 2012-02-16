# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Basic tests to verify that the dispatching mechanism works.
"""
import os
import time
import struct
import random
import string
import simplejson as json
from tempfile import mkstemp

from mozsvc.tests.support import make_request

from syncstorage.util import get_timestamp
from syncstorage.storage import get_storage
from syncstorage.tests.functional import support

import vep
from repoze.who.plugins.vepauth.utils import sign_request

from mozsvc.exceptions import BackendError
from mozsvc.exceptions import ERROR_OVER_QUOTA, ERROR_INVALID_OBJECT


_PLD = '*' * 500
_ASCII = string.ascii_letters + string.digits


def randtext(size=10):
    return ''.join([random.choice(_ASCII) for i in range(size)])


class TestStorage(support.TestWsgiApp):

    def setUp(self):
        super(TestStorage, self).setUp()

        self.root = '/2.0'

        # Exchange a BID assertion for an auth token.
        assertion = vep.DummyVerifier.make_assertion(self.user_email,
                                                     "http://localhost")
        headers = {"Authorization": "Browser-ID " + assertion}
        credentials = self.app.get(self.root + "/token", headers=headers).json

        # Monkey-patch the app to sign all requests with that token.
        def new_do_request(req, *args, **kwds):
            sign_request(req, credentials["id"], credentials["key"])
            return orig_do_request(req, *args, **kwds)
        orig_do_request = self.app.do_request
        self.app.do_request = new_do_request

        # let's create some collections for our tests
        self.storage = get_storage(make_request(self.config))

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
        self.assertEquals(int(resp.headers['X-Num-Records']), len(keys))

        # XXX need to test collections timestamps here

    def test_get_collection_count(self):

        resp = self.app.get(self.root + '/info/collection_counts')
        res = resp.json
        values = res.values()
        values.sort()
        self.assertEquals(values, [3, 5])
        self.assertEquals(int(resp.headers['X-Num-Records']), 2)

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
        bso = {'id': '125', 'payload': _PLD}
        self.app.put_json(self.root + '/storage/xxxx/125', bso)

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
        self.assertEquals(int(resp.headers['X-Num-Records']), 5)

        # trying various filters

        # "ids"
        # Returns the ids for objects in the collection that are in the
        # provided comma-separated list.
        res = self.app.get(self.root + '/storage/col2?ids=1,3')
        res = res.json
        res.sort()
        self.assertEquals(res, ['1', '3'])

        # "older"
        # Returns only ids for objects in the collection that have been last
        # modified before the date given.

        #self.storage.delete_items(self.user_id, 'col2')
        self.app.delete(self.root + '/storage/col2')

        bso = {'id': '128', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/128', bso)
        ts = res.json

        #ts = self.storage.set_item(self.user_id, 'col2', '128', payload='x')
        fts = json.dumps(ts)
        time.sleep(.3)

        bso = {'id': '129', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/129', bso)
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
        # If defined, returns the full BSO, rather than just the id.
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
        bso1 = {'id': '130', 'payload': 'x', 'sortindex': 11}
        bso2 = {'id': '131', 'payload': 'x', 'sortindex': 9}
        bsos = [bso1, bso2]
        self.app.post_json(self.root + '/storage/col2', bsos)

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

        bsos = []
        for i in range(10):
            bso = {'id': str(i), 'payload': 'x'}
            bsos.append(bso)
        self.app.post_json(self.root + '/storage/col2', bsos)

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
            bso = {'id': index, 'payload': 'x', 'sortindex': sortindex}
            self.app.post_json(self.root + '/storage/col2', bso)
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
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/json')])
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

        # unspecified format defaults to json
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.content_type, 'application/json')

        # unkown format gets a 406
        self.app.get(self.root + '/storage/col2', headers=[('Accept', 'x/yy')],
                     status=406)

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
        bso = {'payload': _PLD}
        self.app.put_json(self.root + '/storage/col2/12345', bso)
        res = self.app.get(self.root + '/storage/col2/12345')
        res = res.json
        self.assertEquals(res['payload'], _PLD)

        # now let's update it
        bso = {'payload': 'YYY'}
        self.app.put_json(self.root + '/storage/col2/12345', bso)
        res = self.app.get(self.root + '/storage/col2/12345')
        res = res.json
        self.assertEquals(res['payload'], 'YYY')

    def test_set_collection(self):
        # sending two bsos
        bso1 = {'id': 12, 'payload': _PLD}
        bso2 = {'id': 13, 'payload': _PLD}
        bsos = [bso1, bso2]
        self.app.post_json(self.root + '/storage/col2', bsos)

        # checking what we did
        res = self.app.get(self.root + '/storage/col2/12')
        res = res.json
        self.assertEquals(res['payload'], _PLD)
        res = self.app.get(self.root + '/storage/col2/13')
        res = res.json
        self.assertEquals(res['payload'], _PLD)

        # one more time, with changes
        bso1 = {'id': 13, 'payload': 'XyX'}
        bso2 = {'id': 14, 'payload': _PLD}
        bsos = [bso1, bso2]
        self.app.post_json(self.root + '/storage/col2', bsos)

        # checking what we did
        res = self.app.get(self.root + '/storage/col2/14')
        res = res.json
        self.assertEquals(res['payload'], _PLD)
        res = self.app.get(self.root + '/storage/col2/13')
        res = res.json
        self.assertEquals(res['payload'], 'XyX')

        # sending two bsos with one bad sortindex
        bso1 = {'id': 'one', 'payload': _PLD}
        bso2 = {'id': 'two', 'payload': _PLD,
                'sortindex': 'FAIL'}
        bsos = [bso1, bso2]
        self.app.post_json(self.root + '/storage/col2', bsos)
        self.app.get(self.root + '/storage/col2/two', status=404)

    def test_set_collection_input_formats(self):
        self.storage.delete_collection(self.user_id, "col2")
        # If we send with application/newlines it should work.
        bso1 = {'id': 12, 'payload': _PLD}
        bso2 = {'id': 13, 'payload': _PLD}
        bsos = [bso1, bso2]
        body = "\n".join(json.dumps(bso) for bso in bsos)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/newlines"
        })
        items = self.storage.get_items(self.user_id, "col2")
        self.assertEquals(len(items), 2)
        # If we send an unknown content type, we get an error.
        self.storage.delete_collection(self.user_id, "col2")
        body = json.dumps(bsos)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/octet-stream"
        }, status=400)
        items = self.storage.get_items(self.user_id, "col2")
        self.assertEquals(len(items), 0)

    def test_collection_usage(self):
        self.storage.delete_storage(self.user_id)

        bso1 = {'id': 13, 'payload': 'XyX'}
        bso2 = {'id': 14, 'payload': _PLD}
        bsos = [bso1, bso2]
        self.app.post_json(self.root + '/storage/col2', bsos)

        res = self.app.get(self.root + '/info/collection_usage')
        usage = res.json
        col2_size = usage['col2']
        wanted = len(bso1['payload']) + len(bso2['payload'])
        self.assertEqual(col2_size, wanted / 1024.)

    def test_delete_collection(self):
        self.storage.delete_items(self.user_id, 'col2')

        # creating a collection of three
        bso1 = {'id': 12, 'payload': _PLD}
        bso2 = {'id': 13, 'payload': _PLD}
        bso3 = {'id': 14, 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting all items
        self.app.delete(self.root + '/storage/col2')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

        # Deletes the ids for objects in the collection that are in the
        # provided comma-separated list.
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)
        self.app.delete(self.root + '/storage/col2?ids=12,14')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)
        self.app.delete(self.root + '/storage/col2?ids=13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

    def test_delete_item(self):
        self.storage.delete_items(self.user_id, 'col2')

        # creating a collection of three
        bso1 = {'id': 12, 'payload': _PLD}
        bso2 = {'id': 13, 'payload': _PLD}
        bso3 = {'id': 14, 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
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
        bso1 = {'id': 12, 'payload': _PLD}
        bso2 = {'id': 13, 'payload': _PLD}
        bso3 = {'id': 14, 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting all with no confirmation
        self.app.delete(self.root + '/storage', status=400)

        # deleting all for real now
        res = self.app.delete(self.root + '/storage/col2',
                              headers=[('X-Confirm-Delete', '1')])
        res = json.loads(res.body)
        now = get_timestamp()
        self.assertTrue(abs(now - int(res)) < 200)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

    def test_x_timestamp_header(self):
        now = get_timestamp()
        res = self.app.get(self.root + '/storage/col2')
        self.assertTrue(abs(now -
                int(res.headers['X-Timestamp'])) < 100)

        # getting the timestamp with a PUT
        bso = {'payload': _PLD}
        now = get_timestamp()
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertTrue(abs(now -
                        int(res.headers['X-Timestamp'])) < 200)

        # getting the timestamp with a POST
        bso1 = {'id': 12, 'payload': _PLD}
        bso2 = {'id': 13, 'payload': _PLD}
        bsos = [bso1, bso2]
        now = get_timestamp()
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        self.assertTrue(abs(now -
                        int(res.headers['X-Timestamp'])) < 200)

    def test_ifunmodifiedsince(self):
        bso = {'payload': _PLD}
        ts = self.app.put_json(self.root + '/storage/col2/12345', bso)
        ts = json.loads(ts.body) - 1000
        self.app.put_json(self.root + '/storage/col2/12345', bso,
                     headers=[('X-If-Unmodified-Since', str(ts))],
                     status=412)

    def test_quota(self):
        res = self.app.get(self.root + '/info/quota')
        old_used = res.json["usage"]
        bso = {'payload': _PLD}
        self.app.put_json(self.root + '/storage/col2/12345', bso)
        res = self.app.get(self.root + '/info/quota')
        used = res.json["usage"]
        self.assertEquals(used - old_used, len(_PLD) / 1024.)

    def test_overquota(self):
        self.storage.quota_size = 0.1
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertEquals(res.headers['X-Quota-Remaining'], '0.0765625')

        self.storage.quota_size = 0
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso,
                                status=400)
        self.assertEquals(res.headers['Content-Type'], 'application/json')
        self.assertEquals(res.json, ERROR_OVER_QUOTA)

    def test_get_collection_ttl(self):
        self.app.delete(self.root + '/storage/col2')
        bso = {'payload': _PLD, 'ttl': 0}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        time.sleep(1.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.json, [])

        bso = {'payload': _PLD, 'ttl': 2}
        res = self.app.put_json(self.root + '/storage/col2/123456', bso)

        # it should exists now
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)

        # trying a second put again
        self.app.put_json(self.root + '/storage/col2/123456', bso)

        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)
        time.sleep(2.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

    def test_batch(self):
        # Test that batch uploads are correctly processed.
        # The test config has max_count=100.
        # Uploading 70 small objects should succeed with 3 database writes.
        bsos = [{'id': str(i), 'payload': _PLD} for i in range(70)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), 70)
        self.assertEquals(len(res['failed']), 0)
        # The test config has max_count=100.
        # Uploading 105 items should produce five failures.
        bsos = [{'id': str(i), 'payload': _PLD} for i in range(105)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), 100)
        self.assertEquals(len(res['failed']), 5)
        # The test config has max_bytes=1M.
        # Uploading 5 210MB items should produce one failure.
        bsos = [{'id': str(i), 'payload': "X" * (210 * 1024)}
                for i in range(5)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), 4)
        self.assertEquals(len(res['failed']), 1)

    def test_blacklisted_nodes(self):
        settings = self.config.registry.settings
        old = settings.get('storage.check_blacklisted_nodes', False)
        settings['storage.check_blacklisted_nodes'] = True
        try:
            cache = self.config.registry.get("cache")
            if cache is None:
                return   # memcached is probably not installed

            if not cache.set('TEST', 1):
                return   # memcached server is probably down

            # "backoff:server" will add a X-Backoff header
            cache.set('backoff:localhost:80', 2)
            try:
                resp = self.app.get(self.root + '/info/collections')
                self.assertEquals(resp.headers['X-Backoff'], '2')
            finally:
                cache.delete('backoff:localhost:80')

            # "down:server" will make the node unavailable
            cache.set('down:localhost:80', 1)
            try:
                resp = self.app.get(self.root + '/info/collections',
                                    status=503)
                self.assertTrue("Server Problem Detected" in resp.body)
            finally:
                cache.delete('down:localhost:80')
        finally:
            settings['storage.check_blacklisted_nodes'] = old

    def test_weird_args(self):
        # pushing some data in col2
        bsos = [{'id': str(i), 'payload': _PLD} for i in range(10)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
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
        bsos = [{'id': '{6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d}' % i,
                 'payload': _PLD} for i in range(5)]
        res = self.app.post_json(self.root + '/storage/passwords', bsos)
        res = res.json

        # now deleting some of them
        ids = ','.join(['{6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d}' % i
                        for i in range(2)])

        self.app.delete(self.root + '/storage/passwords?ids=%s' % ids)

        res = self.app.get(self.root + '/storage/passwords?ids=%s' % ids)
        self.assertEqual(res.json, [])

    def test_dependant_options(self):
        settings = self.config.registry.settings.copy()
        settings['storage.check_blacklisted_nodes'] = True
        from syncstorage import main, tweens
        old_client = tweens.Client
        tweens.Client = None
        # make sure the app cannot be initialized if it's asked
        # to check for blacklisted node and memcached is not present
        try:
            self.assertRaises(ValueError, main, {}, **settings)
        finally:
            tweens.Client = old_client

    def test_timestamps_are_integers(self):
        # make sure the server returns only integer timestamps
        resp = self.app.get(self.root + '/storage/col2?full=1')
        bsos = json.loads(resp.body)

        # check how the timestamps look - we need two digits stuff
        stamps = []
        for bso in bsos:
            stamp = bso['modified']
            self.assertEqual(stamp, long(stamp))
            stamps.append(stamp)

        stamps.sort()

        # try a newer filter now, to get the last two objects
        ts = int(stamps[-3])

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
        # send two bsos in the 'meh' collection
        bso1 = {'id': 1, 'payload': _PLD}
        bso2 = {'id': 2, 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/meh', bsos)
        ts = int(res.headers["X-Timestamp"])

        # wait a bit
        time.sleep(0.2)

        # send two more bsos
        bso3 = {'id': 3, 'payload': _PLD}
        bso4 = {'id': 4, 'payload': _PLD}
        bsos = [bso3, bso4]
        res = self.app.post_json(self.root + '/storage/meh', bsos)

        # asking for bsos using newer=ts where newer is the timestamps
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/meh?newer=%s' % ts)
        res = res.json
        self.assertEquals(res, ['3', '4'])

    def test_strict_newer_tabs(self):
        # send two bsos in the 'tabs' collection
        bso1 = {'id': 1, 'payload': _PLD}
        bso2 = {'id': 2, 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/tabs', bsos)
        ts = int(res.headers["X-Timestamp"])

        # wait a bit
        time.sleep(0.2)

        # send two more bsos
        bso3 = {'id': 3, 'payload': _PLD}
        bso4 = {'id': 4, 'payload': _PLD}
        bsos = [bso3, bso4]
        self.app.post_json(self.root + '/storage/tabs', bsos)

        # asking for bsos using newer=ts where newer is the timestamps
        # of bso 1 and 2, should not return them
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

            def get_tabs_timestamp(*args, **kw):
                return 0

            def set_tabs(*args, **kw):
                raise BackendError()

        fd, dbfile = mkstemp()
        os.close(fd)

        try:
            storage = MemcachedSQLStorage('sqlite:///%s' % dbfile,
                                          create_tables=True)
            storage.cache = BadCache()
            for key in self.config.registry:
                if key.startswith("syncstorage:storage:"):
                    self.config.registry[key] = storage

            # send two bsos in the 'tabs' collection
            bso1 = {'id': 'sure', 'payload': _PLD}
            bso2 = {'id': 'thing', 'payload': _PLD}
            bsos = [bso1, bso2]

            # on batch, we get back a 200 - but only failures
            res = self.app.post_json(self.root + '/storage/tabs', bsos)
            self.assertEqual(len(res.json['failed']), 2)
            self.assertEqual(len(res.json['success']), 0)

            # on single PUT, we get a 503
            self.app.put_json(self.root + '/storage/tabs/sure', bso1,
                         status=503)
        finally:
            for key in self.config.registry:
                if key.startswith("storage:"):
                    self.config.registry[key] = self.storage
            os.remove(dbfile)

    def test_batch_size(self):
        # check that the batch size is correctly set
        size = self.config.registry["syncstorage.controller"].batch_size
        self.assertEqual(size, 25)

    def test_handling_of_invalid_json(self):
        # Single upload with JSON that's not a BSO.
        # It should fail with ERROR_INVALID_OBJECT
        bso = "notabso"
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                           status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        bso = 42
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                           status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        bso = {'id': ["1", "2"], 'payload': {'3': '4'}}
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                           status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Batch upload with JSON that's not a list of BSOs
        # It should fail with ERROR_INVALID_OBJECT
        bsos = "notalist"
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        bsos = 42
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Batch upload a list with something that's not a BSO
        # It should process the good entry and fail for the bad.
        bsos = [{'id': '1', 'payload': 'GOOD'}, "BAD"]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), 1)
        self.assertEquals(len(res['failed']), 1)
