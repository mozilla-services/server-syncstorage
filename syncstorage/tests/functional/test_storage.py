# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Functional tests for the SyncStorage server protocol.

This file runs tests to ensure the correct operation of the server
as specified in:

    http://docs.services.mozilla.com/storage/apis-2.0.html

If there's an aspect of that spec that's not covered by a test in this file,
consider it a bug.

"""

import unittest2

import sys
import time
import random
import string
import urllib
import webtest
import simplejson as json

from syncstorage.util import get_timestamp
from syncstorage.tests.functional.support import StorageFunctionalTestCase
from syncstorage.tests.functional.support import run_live_functional_tests
from syncstorage.controller import MAX_IDS_PER_BATCH

from mozsvc.exceptions import BackendError
from mozsvc.exceptions import ERROR_OVER_QUOTA, ERROR_INVALID_OBJECT


_PLD = '*' * 500
_ASCII = string.ascii_letters + string.digits


def randtext(size=10):
    return ''.join([random.choice(_ASCII) for i in range(size)])


class TestStorage(StorageFunctionalTestCase):
    """Storage testcases that only use the web API.

    These tests are suitable for running against both in-process and live
    external web servers.
    """

    def setUp(self):
        super(TestStorage, self).setUp()
        self.root = '/2.0/%d' % (self.user_id,)
        # Reset the storage to a known state, aka "empty".
        self.app.delete(self.root + "/storage")

    def test_get_collections(self):
        # col1 gets 3 items, col2 gets 5 items.
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(3)]
        self.app.post_json(self.root + "/storage/col1", bsos)
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        # only those collections should appear in the query.
        resp = self.app.get(self.root + '/info/collections')
        res = resp.json
        keys = sorted(res.keys())
        self.assertEquals(keys, ["col1", "col2"])
        self.assertEquals(resp.headers.get('X-Num-Records'), None)

    def test_get_collection_count(self):
        # col1 gets 3 items, col2 gets 5 items.
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(3)]
        self.app.post_json(self.root + "/storage/col1", bsos)
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        # those counts should be reflected back in query.
        resp = self.app.get(self.root + '/info/collection_counts')
        res = resp.json
        self.assertEquals(len(res), 2)
        self.assertEquals(res["col1"], 3)
        self.assertEquals(res["col2"], 5)
        self.assertEquals(resp.headers.get('X-Num-Records'), None)

    def test_bad_cache(self):
        # fixes #637332
        # the collection name <-> id mapper is temporarely cached to
        # save a few requests.
        # but should get purged when new collections are added

        # 1. get collection info
        resp = self.app.get(self.root + '/info/collections')
        numcols = len(resp.json)

        # 2. add a new collection + stuff
        bso = {'id': '125', 'payload': _PLD}
        self.app.put_json(self.root + '/storage/xxxx/125', bso)

        # 3. get collection info again, should find the new ones
        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(len(resp.json), numcols + 1)

    def test_get_collection(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)

        # try just getting all items at once.
        resp = self.app.get(self.root + '/storage/col2')
        res = resp.json["items"]
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])
        self.assertEquals(int(resp.headers['X-Num-Records']), 5)

        # trying various filters

        # "ids"
        # Returns the ids for objects in the collection that are in the
        # provided comma-separated list.
        res = self.app.get(self.root + '/storage/col2?ids=1,3')
        res = res.json["items"]
        res.sort()
        self.assertEquals(res, ['1', '3'])

        # "older"
        # Returns only ids for objects in the collection that have been last
        # modified before the date given.

        self.app.delete(self.root + '/storage/col2')

        bso = {'id': '128', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/128', bso)
        ts = int(res.headers["X-Last-Modified"])

        time.sleep(.3)

        bso = {'id': '129', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/129', bso)
        ts2 = int(res.headers["X-Last-Modified"])

        self.assertTrue(ts < ts2)

        res = self.app.get(self.root + '/storage/col2?older=%s' % ts2)
        res = res.json["items"]
        self.assertEquals(res, ['128'])

        res = self.app.get(self.root + '/storage/col2?older=%s' % ts)
        res = res.json["items"]
        self.assertEquals(res, [])

        res = self.app.get(self.root + '/storage/col2?older=%s' % (ts2 + 1))
        res = res.json["items"]
        self.assertEquals(sorted(res), ["128", "129"])

        # "newer"
        # Returns only ids for objects in the collection that have been
        # last modified since the date given.
        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts)
        res = res.json["items"]
        self.assertEquals(res, ['129'])

        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts2)
        res = res.json["items"]
        self.assertEquals(res, [])

        res = self.app.get(self.root + '/storage/col2?newer=%s' % (ts - 1))
        res = res.json["items"]
        self.assertEquals(sorted(res), ['128', '129'])

        # "full"
        # If defined, returns the full BSO, rather than just the id.
        res = self.app.get(self.root + '/storage/col2?full=1')
        res = res.json["items"]
        keys = res[0].keys()
        keys.sort()
        wanted = ['id', 'modified', 'payload']
        self.assertEquals(keys, wanted)

        res = self.app.get(self.root + '/storage/col2')
        res = res.json["items"]
        self.assertTrue(isinstance(res, list))

        # "limit"
        # Sets the maximum number of ids that will be returned
        self.app.delete(self.root + '/storage/col2')

        bsos = []
        for i in range(10):
            bso = {'id': str(i), 'payload': 'x', 'sortindex': i}
            bsos.append(bso)
        self.app.post_json(self.root + '/storage/col2', bsos)

        query_url = self.root + '/storage/col2?sort=index'
        res = self.app.get(query_url)
        all_items = res.json["items"]
        self.assertEquals(len(all_items), 10)

        res = self.app.get(query_url + '&limit=2')
        self.assertEquals(res.json["items"], all_items[:2])

        # "offset"
        # Skips over items that have already been returned.
        next_offset = res.headers["X-Next-Offset"]
        res = self.app.get(query_url + '&limit=3&offset=' + next_offset)
        self.assertEquals(res.json["items"], all_items[2:5])

        next_offset = res.headers["X-Next-Offset"]
        res = self.app.get(query_url + '&offset=' + next_offset)
        self.assertEquals(res.json["items"], all_items[5:])
        self.assertTrue("X-Next-Offset" not in res.headers)

        res = self.app.get(query_url + '&limit=10000&offset=' + next_offset)
        self.assertEquals(res.json["items"], all_items[5:])
        self.assertTrue("X-Next-Offset" not in res.headers)

        # "sort"
        #   'oldest' - Orders by modification date (oldest first)
        #   'newest' - Orders by modification date (newest first)
        #   'index' - Orders by the sortindex descending (highest weight first)
        self.app.delete(self.root + '/storage/col2')

        for index, sortindex in (('0', 1), ('1', 34), ('2', 12)):
            bso = {'id': index, 'payload': 'x', 'sortindex': sortindex}
            self.app.post_json(self.root + '/storage/col2', [bso])
            time.sleep(0.1)

        res = self.app.get(self.root + '/storage/col2?sort=oldest')
        res = res.json["items"]
        self.assertEquals(res, ['0', '1', '2'])

        res = self.app.get(self.root + '/storage/col2?sort=newest')
        res = res.json["items"]
        self.assertEquals(res, ['2', '1', '0'])

        res = self.app.get(self.root + '/storage/col2?sort=index')
        res = res.json["items"]
        self.assertEquals(res, ['1', '2', '0'])

    def test_alternative_formats(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)

        # application/json
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/json')])
        self.assertEquals(res.content_type.split(";")[0], 'application/json')

        res = res.json["items"]
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

        # application/newlines
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/newlines')])
        self.assertEquals(res.content_type, 'application/newlines')

        res = [json.loads(line) for line in res.body.strip().split('\n')]
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

        # unspecified format defaults to json
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.content_type.split(";")[0], 'application/json')

        # unkown format gets a 406
        self.app.get(self.root + '/storage/col2', headers=[('Accept', 'x/yy')],
                     status=406)

    def test_set_collection_with_if_modified_since(self):
        # Create five items with different timestamps.
        for i in xrange(5):
            bsos = [{"id": str(i), "payload": "xxx"}]
            self.app.post_json(self.root + "/storage/col2", bsos)
            time.sleep(0.01)
        # Get them all, along with their timestamps.
        res = self.app.get(self.root + '/storage/col2?full=true').json["items"]
        self.assertEquals(len(res), 5)
        timestamps = sorted([r["modified"] for r in res])
        # The ts of the collection should be the max ts of those items.
        self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since": str(timestamps[0])
        }, status=200)
        self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since": str(timestamps[-1])
        }, status=304)

    def test_get_item(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        # grabbing object 1 from col2
        res = self.app.get(self.root + '/storage/col2/1')
        res = res.json
        keys = res.keys()
        keys.sort()
        self.assertEquals(keys, ['id', 'modified', 'payload'])
        self.assertEquals(res['id'], '1')

        # unexisting object
        self.app.get(self.root + '/storage/col2/99', status=404)

        # using x-if-modified-since header.
        self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since": str(res["modified"])
        }, status=304)
        self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since": str(res["modified"] + 10)
        }, status=304)
        res = self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since": str(res["modified"] - 10)
        })
        self.assertEquals(res.json['id'], '1')

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
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
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
        bso1 = {'id': '13', 'payload': 'XyX'}
        bso2 = {'id': '14', 'payload': _PLD}
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
        # If we send with application/newlines it should work.
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bsos = [bso1, bso2]
        body = "\n".join(json.dumps(bso) for bso in bsos)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/newlines"
        })
        items = self.app.get(self.root + "/storage/col2").json["items"]
        self.assertEquals(len(items), 2)
        # If we send an unknown content type, we get an error.
        self.app.delete(self.root + "/storage/col2")
        body = json.dumps(bsos)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/octet-stream"
        }, status=415)
        self.app.get(self.root + "/storage/col2", status=404)

    def test_set_item_input_formats(self):
        # If we send with application/json it should work.
        body = json.dumps({'payload': _PLD})
        self.app.put(self.root + '/storage/col2/TEST', body, headers={
            "Content-Type": "application/json"
        })
        item = self.app.get(self.root + "/storage/col2/TEST").json
        self.assertEquals(item["payload"], _PLD)
        # If we send json with some other content type, it should fail
        self.app.delete(self.root + "/storage/col2")
        self.app.put(self.root + '/storage/col2/TEST', body, headers={
            "Content-Type": "application/octet-stream"
        }, status=415)
        self.app.get(self.root + "/storage/col2/TEST", status=404)

    def test_app_newlines_when_payloads_contain_newlines(self):
        # Send some application/newlines with embedded newline chars.
        bsos = [
            {'id': '1', 'payload': 'hello\nworld'},
            {'id': '2', 'payload': '\nmarco\npolo\n'},
        ]
        body = "\n".join(json.dumps(bso) for bso in bsos)
        self.assertEquals(len(body.split("\n")), 2)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/newlines"
        })
        # Read them back as JSON list, check payloads.
        items = self.app.get(self.root + "/storage/col2?full=1").json["items"]
        self.assertEquals(len(items), 2)
        items.sort(key=lambda bso: bso["id"])
        self.assertEquals(items[0]["payload"], bsos[0]["payload"])
        self.assertEquals(items[1]["payload"], bsos[1]["payload"])
        # Read them back as application/newlines, check payloads.
        res = self.app.get(self.root + "/storage/col2?full=1", headers={
          "Accept": "application/newlines",
        })
        items = [json.loads(line) for line in res.body.strip().split('\n')]
        self.assertEquals(len(items), 2)
        items.sort(key=lambda bso: bso["id"])
        self.assertEquals(items[0]["payload"], bsos[0]["payload"])
        self.assertEquals(items[1]["payload"], bsos[1]["payload"])

    def test_collection_usage(self):
        self.app.delete(self.root + "/storage")

        bso1 = {'id': '13', 'payload': 'XyX'}
        bso2 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2]
        self.app.post_json(self.root + '/storage/col2', bsos)

        res = self.app.get(self.root + '/info/collection_usage')
        usage = res.json
        col2_size = usage['col2']
        wanted = len(bso1['payload']) + len(bso2['payload'])
        self.assertEqual(col2_size, wanted)

    def test_delete_collection_items(self):
        # creating a collection of three
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bso3 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 3)

        # deleting all items
        self.app.delete(self.root + '/storage/col2')
        self.app.get(self.root + '/storage/col2', status=404)

        # Deletes the ids for objects in the collection that are in the
        # provided comma-separated list.
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 3)
        self.app.delete(self.root + '/storage/col2?ids=12,14')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 1)
        self.app.delete(self.root + '/storage/col2?ids=13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 0)

    def test_delete_item(self):
        # creating a collection of three
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bso3 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 3)

        # deleting item 13
        self.app.delete(self.root + '/storage/col2/13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 2)

        # unexisting item should return a 404
        self.app.delete(self.root + '/storage/col2/12982', status=404)

    def test_delete_storage(self):
        # creating a collection of three
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bso3 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 3)

        # deleting all
        self.app.delete(self.root + '/storage')
        res = self.app.delete(self.root + '/storage/col2', status=404)
        self.app.get(self.root + '/storage/col2', status=404)

    def test_x_timestamp_header(self):
        # This can't be run against a live server.
        if self.distant:
            raise unittest2.SkipTest

        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)

        now = get_timestamp()
        time.sleep(0.001)
        res = self.app.get(self.root + '/storage/col2')
        self.assertTrue(now < int(res.headers['X-Timestamp']))

        # getting the timestamp with a PUT
        now = get_timestamp()
        time.sleep(0.001)
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertTrue(now < int(res.headers['X-Timestamp']))
        self.assertTrue(abs(now -
                        int(res.headers['X-Timestamp'])) < 200)

        # getting the timestamp with a POST
        now = get_timestamp()
        time.sleep(0.001)
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        self.assertTrue(now < int(res.headers['X-Timestamp']))

    def test_ifunmodifiedsince(self):
        bso = {'id': '12345', 'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        # Using an X-If-Unmodified-Since in the past should cause 412s.
        ts = str(int(res.headers['X-Last-Modified']) - 1000)
        bso = {'id': '12345', 'payload': _PLD + "XXX"}
        self.app.put_json(self.root + '/storage/col2/12345', bso,
                          headers=[('X-If-Unmodified-Since', ts)],
                          status=412)
        self.app.delete(self.root + '/storage/col2/12345',
                        headers=[('X-If-Unmodified-Since', ts)],
                        status=412)
        self.app.post_json(self.root + '/storage/col2', [bso],
                           headers=[('X-If-Unmodified-Since', ts)],
                           status=412)
        self.app.delete(self.root + '/storage/col2?ids=12345',
                        headers=[('X-If-Unmodified-Since', ts)],
                        status=412)
        self.app.get(self.root + '/storage/col2/12345',
                          headers=[('X-If-Unmodified-Since', ts)],
                          status=412)
        self.app.get(self.root + '/storage/col2',
                          headers=[('X-If-Unmodified-Since', ts)],
                          status=412)
        # Deleting items from a collection should give 412 even if some
        # other, unrelated item in the collection has been modified.
        ts = res.headers['X-Last-Modified']
        time.sleep(0.001)
        res2 = self.app.put_json(self.root + '/storage/col2/54321', {
                                    'payload': _PLD,
                                 })
        self.app.delete(self.root + '/storage/col2?ids=12345',
                        headers=[('X-If-Unmodified-Since', ts)],
                        status=412)
        ts = res2.headers['X-Last-Modified']
        # All of those should have left the BSO unchanged
        res2 = self.app.get(self.root + '/storage/col2/12345')
        self.assertEquals(res2.json['payload'], _PLD)
        self.assertEquals(res2.headers['X-Last-Modified'],
                          res.headers['X-Last-Modified'])
        # Using an X-If-Unmodified-Since equal to X-Last-Modified should
        # allow the request to succeed.
        res = self.app.post_json(self.root + '/storage/col2', [bso],
                                 headers=[('X-If-Unmodified-Since', ts)],
                                 status=200)
        ts = res.headers['X-Last-Modified']
        self.app.get(self.root + '/storage/col2/12345',
                          headers=[('X-If-Unmodified-Since', ts)],
                          status=200)
        self.app.delete(self.root + '/storage/col2/12345',
                        headers=[('X-If-Unmodified-Since', ts)],
                        status=204)
        res = self.app.put_json(self.root + '/storage/col2/12345', bso,
                                headers=[('X-If-Unmodified-Since', '0')],
                                status=201)
        ts = res.headers['X-Last-Modified']
        self.app.get(self.root + '/storage/col2',
                          headers=[('X-If-Unmodified-Since', ts)],
                          status=200)
        self.app.delete(self.root + '/storage/col2?ids=12345',
                        headers=[('X-If-Unmodified-Since', ts)],
                        status=204)

    def test_quota(self):
        res = self.app.get(self.root + '/info/quota')
        old_used = res.json["usage"]
        bso = {'payload': _PLD}
        self.app.put_json(self.root + '/storage/col2/12345', bso)
        res = self.app.get(self.root + '/info/quota')
        used = res.json["usage"]
        self.assertEquals(used - old_used, len(_PLD))

    def test_overquota(self):
        # This can't be run against a live server.
        if self.distant:
            raise unittest2.SkipTest

        # Clear out any data that's already in the store.
        self.app.delete(self.root + "/storage")

        # Set a low quota for the storage.
        self.config.registry["syncstorage.controller"].quota_size = 700

        # Check the the remaining quota is correctly reported.
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertEquals(res.headers['X-Quota-Remaining'], '200')

        # Set the quota so that they're over their limit.
        self.config.registry["syncstorage.controller"].quota_size = 10
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso,
                                status=400)
        self.assertEquals(res.content_type.split(";")[0], 'application/json')
        self.assertEquals(res.json, ERROR_OVER_QUOTA)

    def test_get_collection_ttl(self):
        bso = {'payload': _PLD, 'ttl': 0}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        time.sleep(1.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.json["items"], [])

        bso = {'payload': _PLD, 'ttl': 2}
        res = self.app.put_json(self.root + '/storage/col2/123456', bso)

        # it should exists now
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 1)

        # trying a second put again
        self.app.put_json(self.root + '/storage/col2/123456', bso)

        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 1)
        time.sleep(2.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json["items"]), 0)

    def test_batch(self):
        # Test that batch uploads are correctly processed.
        # The test config has max_count=100.
        # Uploading 70 small objects should succeed.
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
        # This can't be run against a live server.
        # XXX TODO: it really doesn't belong in this file; maybe test_wsgi.py?
        if self.distant:
            raise unittest2.SkipTest

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
        args = ('older', 'newer', 'limit', 'offset')
        for arg in args:
            self.app.get(self.root + '/storage/col2?%s=%s' % (arg, randtext()),
                         status=400)

        # what about a crazy ids= string ?
        ids = ','.join([randtext(100) for i in range(10)])
        res = self.app.get(self.root + '/storage/col2?ids=%s' % ids)
        self.assertEquals(res.json["items"], [])

        # trying unexpected args - they should not break
        self.app.get(self.root + '/storage/col2?blabla=1',
                     status=200)

    def test_guid_deletion(self):
        # pushing some data in col2
        bsos = [{'id': '6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d' % i,
                 'payload': _PLD} for i in range(5)]
        res = self.app.post_json(self.root + '/storage/passwords', bsos)
        res = res.json
        self.assertEquals(len(res["success"]), 5)

        # now deleting some of them
        ids = ','.join(['6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d' % i
                        for i in range(2)])

        self.app.delete(self.root + '/storage/passwords?ids=%s' % ids)

        res = self.app.get(self.root + '/storage/passwords?ids=%s' % ids)
        self.assertEqual(len(res.json["items"]), 0)
        res = self.app.get(self.root + '/storage/passwords')
        self.assertEqual(len(res.json["items"]), 3)

    def test_specifying_ids_with_percent_encoded_query_string(self):
        # create some items
        bsos = [{'id': 'test-%d' % i, 'payload': _PLD} for i in range(5)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res["success"]), 5)
        # now delete some of them
        ids = ','.join(['test-%d' % i for i in range(2)])
        ids = urllib.quote(ids)
        self.app.delete(self.root + '/storage/col2?ids=%s' % ids)
        # check that the correct items were deleted
        res = self.app.get(self.root + '/storage/col2?ids=%s' % ids)
        self.assertEqual(len(res.json["items"]), 0)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEqual(len(res.json["items"]), 3)

    def test_timestamps_are_integers(self):
        # Create five items with different timestamps.
        for i in xrange(5):
            bsos = [{"id": str(i), "payload": "xxx"}]
            self.app.post_json(self.root + "/storage/col2", bsos)
            time.sleep(0.01)

        # make sure the server returns only integer timestamps
        resp = self.app.get(self.root + '/storage/col2?full=1')
        bsos = json.loads(resp.body)["items"]
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
        res = res.json["items"]
        try:
            self.assertEquals(sorted(res), ['3', '4'])
        except AssertionError:
            # need to display the whole collection to understand the issue
            msg = 'Stamp used: %s' % ts
            msg += ' ' + self.app.get(self.root + '/storage/col2?full=1').body
            msg += ' Stamps received: %s' % str(stamps)
            raise AssertionError(msg)

    def test_strict_newer(self):
        # send two bsos in the 'meh' collection
        bso1 = {'id': '1', 'payload': _PLD}
        bso2 = {'id': '2', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/meh', bsos)
        ts = int(res.headers["X-Last-Modified"])

        # wait a bit
        time.sleep(0.2)

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        res = self.app.post_json(self.root + '/storage/meh', bsos)

        # asking for bsos using newer=ts where newer is the timestamps
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/meh?newer=%s' % ts)
        res = res.json["items"]
        self.assertEquals(sorted(res), ['3', '4'])

    def test_handling_of_invalid_json_in_bso_uploads(self):
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

    def test_handling_of_invalid_bso_fields(self):
        coll_url = self.root + "/storage/col2"
        # Invalid ID - unacceptable characters.
        bso = {"id": "A,B", "payload": "testing"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        # Invalid ID - empty string is not acceptable.
        bso = {"id": "", "payload": "testing"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        # Invalid ID - too long
        bso = {"id": "X" * 65, "payload": "testing"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Invalid sortindex - not an integer
        bso = {"id": "TEST", "payload": "testing", "sortindex": "meh"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Invalid sortindex - not an integer
        bso = {"id": "TEST", "payload": "testing", "sortindex": "2.6"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Invalid sortindex - larger than max value
        bso = {"id": "TEST", "payload": "testing", "sortindex": "1" + "0" * 9}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Invalid payload - not a string
        bso = {"id": "TEST", "payload": 42}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Invalid ttl - not an integer
        bso = {"id": "TEST", "payload": "testing", "ttl": "eh?"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)
        # Invalid ttl - not an integer
        bso = {"id": "TEST", "payload": "testing", "ttl": "4.2"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        # Invalid BSO - unknown field
        bso = {"id": "TEST", "unexpected": "spanish-inquisition"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(int(res.body), ERROR_INVALID_OBJECT)

    def test_generation_of_201_and_204_response_codes(self):
        bso = {"id": "TEST", "payload": "testing"}
        # If a new BSO is created, the return code should be 201.
        self.app.put_json(self.root + "/storage/col2/TEST", bso, status=201)
        # If an existing BSO is updated, the return code should be 204.
        bso["payload"] = "testing_again"
        self.app.put_json(self.root + "/storage/col2/TEST", bso, status=204)

    def test_that_batch_deletes_are_limited_to_max_number_of_items(self):
        bso = {"id": "1", "payload": "testing"}
        # Deleting with less than the limit works OK.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(MAX_IDS_PER_BATCH - 1))
        self.app.delete(self.root + "/storage/col2?ids=" + ids, status=204)
        # Deleting with equal to the limit works OK.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(MAX_IDS_PER_BATCH))
        self.app.delete(self.root + "/storage/col2?ids=" + ids, status=204)
        # Deleting with more than the limit fails.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(MAX_IDS_PER_BATCH + 1))
        self.app.delete(self.root + "/storage/col2?ids=" + ids, status=400)

    def test_that_expired_items_can_be_overwritten_via_PUT(self):
        # Upload something with a small ttl.
        bso = {"payload": "XYZ", "ttl": 0}
        self.app.put_json(self.root + "/storage/col2/TEST", bso)
        # Wait for it to expire.
        time.sleep(0.02)
        self.app.get(self.root + "/storage/col2/TEST", status=404)
        # Overwriting it should still work.
        bso = {"payload": "XYZ", "ttl": 42}
        self.app.put_json(self.root + "/storage/col2/TEST", bso)

    def test_if_modified_since_on_info_views(self):
        # Store something, so the views have a modified time > 0.
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(3)]
        self.app.post_json(self.root + "/storage/col1", bsos)
        INFO_VIEWS = ("/info/collections", "/info/quota",
                      "/info/collection_usage", "/info/collection_counts")
        # Get the initial modified time.
        r = self.app.get(self.root + "/info/collections")
        ts1 = r.headers["X-Last-Modified"]
        self.assertTrue(int(ts1) > 3)
        # With X-I-M-S set before latest change, all should give a 200.
        headers = {"X-If-Modified-Since": "3"}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=200)
        # With X-I-M-S set to after latest change , all should give a 304.
        headers = {"X-If-Modified-Since": str(ts1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=304)
        # Change a collection.
        time.sleep(0.01)
        bso = {"payload": "TEST"}
        r = self.app.put_json(self.root + "/storage/col2/TEST", bso)
        ts2 = r.headers["X-Last-Modified"]
        # Using the previous timestamp should read the updated data.
        headers = {"X-If-Modified-Since": str(ts1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=200)
        # Using the new timestamp should produce 304s.
        headers = {"X-If-Modified-Since": str(ts2)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=304)
        # XXX TODO: the storage-level timestamp is not tracked correctly
        # after deleting a collection, so this test fails for now.
        ## Delete a collection.
        #time.sleep(0.01)
        #r = self.app.delete(self.root + "/storage/col2")
        #ts3 = r.headers["X-Timestamp"]
        ## Using the previous timestamp should read the updated data.
        #headers = {"X-If-Modified-Since": str(ts2)}
        #for view in INFO_VIEWS:
        #    self.app.get(self.root + view, headers=headers, status=200)
        ## Using the new timestamp should produce 304s.
        #headers = {"X-If-Modified-Since": str(ts3)}
        #for view in INFO_VIEWS:
        #    self.app.get(self.root + view, headers=headers, status=304)

    def test_that_x_last_modified_is_sent_for_all_get_requests(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        r = self.app.get(self.root + "/info/collections")
        self.assertTrue("X-Last-Modified" in r.headers)
        r = self.app.get(self.root + "/info/collection_counts")
        self.assertTrue("X-Last-Modified" in r.headers)
        r = self.app.get(self.root + "/storage/col2")
        self.assertTrue("X-Last-Modified" in r.headers)
        r = self.app.get(self.root + "/storage/col2/1")
        self.assertTrue("X-Last-Modified" in r.headers)


class TestStorageMemcached(TestStorage):
    """Storage testcases run against the memcached backend, if available."""

    TEST_INI_FILE = "tests-memcached.ini"

    def setUp(self):
        # If we can't initialize due to an ImportError or BackendError,
        # assume that memcache is unavailable and skip the test.
        try:
            super(TestStorageMemcached, self).setUp()
        except (ImportError, BackendError):
            raise unittest2.SkipTest()
        except webtest.AppError, e:
            if "503" not in str(e):
                raise
            raise unittest2.SkipTest()

    # Memcache backend is configured to store tabs in cache only.
    # Add some tests the see if they still behave correctly.

    def test_strict_newer_tabs(self):
        # send two bsos in the 'tabs' collection
        bso1 = {'id': '1', 'payload': _PLD}
        bso2 = {'id': '2', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/tabs', bsos)
        ts = int(res.headers["X-Last-Modified"])

        # wait a bit
        time.sleep(0.2)

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        self.app.post_json(self.root + '/storage/tabs', bsos)

        # asking for bsos using newer=ts where newer is the timestamps
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/tabs?newer=%s' % ts)
        res = res.json["items"]
        self.assertEquals(res, ['3', '4'])

    def test_write_tabs_503(self):
        # This can't be run against a live server.
        if self.distant:
            raise unittest2.SkipTest

        class BadCache(object):
            """Cache client stub that raises BackendError on write."""

            def __init__(self, cache):
                self.cache = cache

            def cas(self, key, *args, **kw):
                if key.endswith(":tabs"):
                    raise BackendError()
                return self.cache.cas(key, *args, **kw)

            def __getattr__(self, attr):
                return getattr(self.cache, attr)

        try:
            for key in self.config.registry:
                if key.startswith("syncstorage:storage:"):
                    storage = self.config.registry[key]
                    storage.cache = BadCache(storage.cache)

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
                if key.startswith("syncstorage:storage:"):
                    storage = self.config.registry[key]
                    if isinstance(storage.cache, BadCache):
                        storage.cache = storage.cache.cache


if __name__ == "__main__":
    # When run as a script, this file will execute the
    # functional tests against a live webserver.
    res = run_live_functional_tests(TestStorage, sys.argv)
    sys.exit(res)
