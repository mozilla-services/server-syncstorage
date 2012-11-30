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

from syncstorage.tests.functional.support import StorageFunctionalTestCase
from syncstorage.tests.functional.support import run_live_functional_tests
from syncstorage.views.validators import BATCH_MAX_COUNT

from mozsvc.exceptions import BackendError


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
        # modified before the version given.

        self.app.delete(self.root + '/storage/col2')

        bso = {'id': '128', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/128', bso)
        ver1 = int(res.headers["X-Last-Modified-Version"])

        bso = {'id': '129', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/129', bso)
        ver2 = int(res.headers["X-Last-Modified-Version"])

        self.assertTrue(ver1 < ver2)

        res = self.app.get(self.root + '/storage/col2?older=%s' % ver2)
        res = res.json["items"]
        self.assertEquals(res, ['128'])

        res = self.app.get(self.root + '/storage/col2?older=%s' % ver1)
        res = res.json["items"]
        self.assertEquals(res, [])

        res = self.app.get(self.root + '/storage/col2?older=%s' % (ver2 + 1))
        res = res.json["items"]
        self.assertEquals(sorted(res), ["128", "129"])

        # "newer"
        # Returns only ids for objects in the collection that have been
        # last modified since the version given.
        res = self.app.get(self.root + '/storage/col2?newer=%s' % ver1)
        res = res.json["items"]
        self.assertEquals(res, ['129'])

        res = self.app.get(self.root + '/storage/col2?newer=%s' % ver2)
        res = res.json["items"]
        self.assertEquals(res, [])

        res = self.app.get(self.root + '/storage/col2?newer=%s' % (ver1 - 1))
        res = res.json["items"]
        self.assertEquals(sorted(res), ['128', '129'])

        # "full"
        # If defined, returns the full BSO, rather than just the id.
        res = self.app.get(self.root + '/storage/col2?full=1')
        res = res.json["items"]
        keys = res[0].keys()
        keys.sort()
        wanted = ['id', 'payload', 'timestamp', 'version']
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
        #   'oldest' - Orders by version number (oldest first)
        #   'newest' - Orders by version number (newest first)
        #   'index' - Orders by the sortindex descending (highest weight first)
        self.app.delete(self.root + '/storage/col2')

        for index, sortindex in (('0', 1), ('1', 34), ('2', 12)):
            bso = {'id': index, 'payload': 'x', 'sortindex': sortindex}
            self.app.post_json(self.root + '/storage/col2', [bso])

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
        # Get them all, along with their timestamps.
        res = self.app.get(self.root + '/storage/col2?full=true').json["items"]
        self.assertEquals(len(res), 5)
        versions = sorted([r["version"] for r in res])
        # The version of the collection should be the max of those versions.
        self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since-Version": str(versions[0])
        }, status=200)
        res = self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since-Version": str(versions[-1])
        }, status=304)
        self.assertTrue("X-Last-Modified-Version" in res.headers)

    def test_get_item(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        # grabbing object 1 from col2
        res = self.app.get(self.root + '/storage/col2/1')
        res = res.json
        keys = res.keys()
        keys.sort()
        self.assertEquals(keys, ['id', 'payload', 'timestamp', 'version'])
        self.assertEquals(res['id'], '1')

        # unexisting object
        self.app.get(self.root + '/storage/col2/99', status=404)

        # using x-if-modified-since-version header.
        self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since-Version": str(res["version"])
        }, status=304)
        self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since-Version": str(res["version"] + 1)
        }, status=304)
        res = self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since-Version": str(res["version"] - 1)
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

        now = int(time.time() * 1000)
        time.sleep(0.001)
        res = self.app.get(self.root + '/storage/col2')
        self.assertTrue(now < int(res.headers['X-Timestamp']))

        # getting the timestamp with a PUT
        now = int(time.time() * 1000)
        time.sleep(0.001)
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertTrue(now < int(res.headers['X-Timestamp']))
        self.assertTrue(abs(now -
                        int(res.headers['X-Timestamp'])) < 200)

        # getting the timestamp with a POST
        now = int(time.time() * 1000)
        time.sleep(0.001)
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        self.assertTrue(now < int(res.headers['X-Timestamp']))

    def test_ifunmodifiedsince(self):
        bso = {'id': '12345', 'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        # Using an X-If-Unmodified-Since-Version in the past should cause 412s.
        ver = str(int(res.headers['X-Last-Modified-Version']) - 1000)
        bso = {'id': '12345', 'payload': _PLD + "XXX"}
        res = self.app.put_json(
            self.root + '/storage/col2/12345', bso,
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        self.assertTrue("X-Last-Modified-Version" in res.headers)
        res = self.app.delete(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        self.assertTrue("X-Last-Modified-Version" in res.headers)
        self.app.post_json(
            self.root + '/storage/col2', [bso],
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        self.app.delete(
            self.root + '/storage/col2?ids=12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        self.app.get(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        self.app.get(
            self.root + '/storage/col2',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        # Deleting items from a collection should give 412 even if some
        # other, unrelated item in the collection has been modified.
        ver = res.headers['X-Last-Modified-Version']
        res2 = self.app.put_json(self.root + '/storage/col2/54321', {
            'payload': _PLD,
        })
        self.app.delete(
            self.root + '/storage/col2?ids=12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=412)
        ver = res2.headers['X-Last-Modified-Version']
        # All of those should have left the BSO unchanged
        res2 = self.app.get(self.root + '/storage/col2/12345')
        self.assertEquals(res2.json['payload'], _PLD)
        self.assertEquals(res2.headers['X-Last-Modified-Version'],
                          res.headers['X-Last-Modified-Version'])
        # Using an X-If-Unmodified-Since-Version equal to
        # X-Last-Modified-Version should allow the request to succeed.
        res = self.app.post_json(
            self.root + '/storage/col2', [bso],
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=200)
        ver = res.headers['X-Last-Modified-Version']
        self.app.get(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=200)
        self.app.delete(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=204)
        res = self.app.put_json(
            self.root + '/storage/col2/12345', bso,
            headers=[('X-If-Unmodified-Since-Version', '0')],
            status=201)
        ver = res.headers['X-Last-Modified-Version']
        self.app.get(
            self.root + '/storage/col2',
            headers=[('X-If-Unmodified-Since-Version', ver)],
            status=200)
        self.app.delete(
            self.root + '/storage/col2?ids=12345',
            headers=[('X-If-Unmodified-Since-Version', ver)],
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
        self.config.registry.settings["storage.quota_size"] = 700

        # Check the the remaining quota is correctly reported.
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertEquals(res.headers['X-Quota-Remaining'], '200')

        # Set the quota so that they're over their limit.
        self.config.registry.settings["storage.quota_size"] = 10
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso,
                                status=403)
        self.assertEquals(res.content_type.split(";")[0], 'application/json')
        self.assertEquals(res.json["status"], "quota-exceeded")

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
            value = randtext()
            r = self.app.get(self.root + '/storage/col2?%s=%s' % (arg, value),
                             status=400)
            self.assertEquals(r.json["errors"][0]["location"], "querystring")
            self.assertEquals(r.json["errors"][0]["name"], arg)

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

    def test_version_numbers_are_integers(self):
        # Create five items with different version numbers.
        for i in xrange(5):
            bsos = [{"id": str(i), "payload": "xxx"}]
            self.app.post_json(self.root + "/storage/col2", bsos)

        # make sure the server returns only integer version numbers
        resp = self.app.get(self.root + '/storage/col2?full=1')
        bsos = json.loads(resp.body)["items"]
        versions = []
        for bso in bsos:
            ver = bso['version']
            self.assertEqual(ver, long(ver))
            versions.append(ver)

        versions.sort()

        # try a newer filter now, to get the last two objects
        ver = int(versions[-3])

        # Returns only ids for objects in the collection that have been
        # last modified since the version given.
        res = self.app.get(self.root + '/storage/col2?newer=%s' % ver)
        res = res.json["items"]
        try:
            self.assertEquals(sorted(res), ['3', '4'])
        except AssertionError:
            # need to display the whole collection to understand the issue
            msg = 'Version used: %s' % ver
            msg += ' ' + self.app.get(self.root + '/storage/col2?full=1').body
            msg += ' Versions received: %s' % str(versions)
            raise AssertionError(msg)

    def test_strict_newer(self):
        # send two bsos in the 'meh' collection
        bso1 = {'id': '1', 'payload': _PLD}
        bso2 = {'id': '2', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/meh', bsos)
        ver = int(res.headers["X-Last-Modified-Version"])

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        res = self.app.post_json(self.root + '/storage/meh', bsos)

        # asking for bsos using newer=ver where newer is the version
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/meh?newer=%s' % ver)
        res = res.json["items"]
        self.assertEquals(sorted(res), ['3', '4'])

    def test_handling_of_invalid_json_in_bso_uploads(self):
        # Single upload with JSON that's not a BSO.
        bso = "notabso"
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                                status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")

        bso = 42
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                                status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")

        bso = {'id': ["1", "2"], 'payload': {'3': '4'}}
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                                status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")

        # Batch upload with JSON that's not a list of BSOs
        bsos = "notalist"
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bsos")

        bsos = 42
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bsos")

        # Batch upload a list with something that's not a valid data dict.
        # It should fail out entirely, as the input is seriously broken.
        bsos = [{'id': '1', 'payload': 'GOOD'}, "BAD"]
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")

        # Batch upload a list with something that's an invalid BSO.
        # It should process the good entry and fail for the bad.
        bsos = [{'id': '1', 'payload': 'GOOD'}, {'id': '2', 'invalid': 'ya'}]
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
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid sortindex - not an integer
        bso = {"id": "TEST", "payload": "testing", "sortindex": "meh"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid sortindex - not an integer
        bso = {"id": "TEST", "payload": "testing", "sortindex": "2.6"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid sortindex - larger than max value
        bso = {"id": "TEST", "payload": "testing", "sortindex": "1" + "0" * 9}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid payload - not a string
        bso = {"id": "TEST", "payload": 42}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid ttl - not an integer
        bso = {"id": "TEST", "payload": "testing", "ttl": "eh?"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid ttl - not an integer
        bso = {"id": "TEST", "payload": "testing", "ttl": "4.2"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")
        # Invalid BSO - unknown field
        bso = {"id": "TEST", "unexpected": "spanish-inquisition"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json["errors"][0]["location"], "body")
        self.assertEquals(res.json["errors"][0]["name"], "bso")

    def test_generation_of_201_and_204_response_codes(self):
        bso = {"id": "TEST", "payload": "testing"}
        # If a new BSO is created, the return code should be 201.
        self.app.put_json(self.root + "/storage/col2/TEST", bso, status=201)
        # If an existing BSO is updated, the return code should be 204.
        bso["payload"] = "testing_again"
        self.app.put_json(self.root + "/storage/col2/TEST", bso, status=204)

    def test_that_batch_gets_are_limited_to_max_number_of_ids(self):
        bso = {"id": "1", "payload": "testing"}
        self.app.put_json(self.root + "/storage/col2/1", bso)

        # Getting with less than the limit works OK.
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_COUNT - 1))
        res = self.app.get(self.root + "/storage/col2?ids=" + ids)
        self.assertEquals(res.json["items"], ["1"])

        # Getting with equal to the limit works OK.
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_COUNT))
        res = self.app.get(self.root + "/storage/col2?ids=" + ids)
        self.assertEquals(res.json["items"], ["1"])

        # Getting with more than the limit fails.
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_COUNT + 1))
        self.app.get(self.root + "/storage/col2?ids=" + ids, status=400)

    def test_that_batch_deletes_are_limited_to_max_number_of_ids(self):
        bso = {"id": "1", "payload": "testing"}

        # Deleting with less than the limit works OK.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_COUNT - 1))
        self.app.delete(self.root + "/storage/col2?ids=" + ids, status=204)

        # Deleting with equal to the limit works OK.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_COUNT))
        self.app.delete(self.root + "/storage/col2?ids=" + ids, status=204)

        # Deleting with more than the limit fails.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_COUNT + 1))
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
        # Get the initial last-modified version.
        r = self.app.get(self.root + "/info/collections")
        ver1 = int(r.headers["X-Last-Modified-Version"])
        self.assertTrue(ver1 > 0)
        # With X-I-M-S-V set before latest change, all should give a 200.
        headers = {"X-If-Modified-Since-Version": str(ver1 - 1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=200)
        # With X-I-M-S-V set to after latest change , all should give a 304.
        headers = {"X-If-Modified-Since-Version": str(ver1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=304)
        # Change a collection.
        bso = {"payload": "TEST"}
        r = self.app.put_json(self.root + "/storage/col2/TEST", bso)
        ver2 = r.headers["X-Last-Modified-Version"]
        # Using the previous version should read the updated data.
        headers = {"X-If-Modified-Since-Version": str(ver1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=200)
        # Using the new timestamp should produce 304s.
        headers = {"X-If-Modified-Since-Version": str(ver2)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=304)
        # XXX TODO: the storage-level timestamp is not tracked correctly
        # after deleting a collection, so this test fails for now.
        ## Delete a collection.
        #r = self.app.delete(self.root + "/storage/col2")
        #ver3 = r.headers["X-Last-Modified-Version"]
        ## Using the previous timestamp should read the updated data.
        #headers = {"X-If-Modified-Since-Version": str(ver2)}
        #for view in INFO_VIEWS:
        #    self.app.get(self.root + view, headers=headers, status=200)
        ## Using the new timestamp should produce 304s.
        #headers = {"X-If-Modified-Since-Version": str(ver3)}
        #for view in INFO_VIEWS:
        #    self.app.get(self.root + view, headers=headers, status=304)

    def test_that_x_last_modified_is_sent_for_all_get_requests(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        r = self.app.get(self.root + "/info/collections")
        self.assertTrue("X-Last-Modified-Version" in r.headers)
        r = self.app.get(self.root + "/info/collection_counts")
        self.assertTrue("X-Last-Modified-Version" in r.headers)
        r = self.app.get(self.root + "/storage/col2")
        self.assertTrue("X-Last-Modified-Version" in r.headers)
        r = self.app.get(self.root + "/storage/col2/1")
        self.assertTrue("X-Last-Modified-Version" in r.headers)

    def test_update_of_ttl_without_sending_data(self):
        bso = {"payload": "x", "ttl": 1}
        self.app.put_json(self.root + "/storage/col2/TEST1", bso)
        self.app.put_json(self.root + "/storage/col2/TEST2", bso)
        # We can only update via POST, not PUT.
        bso = {"ttl": 10}
        self.app.put_json(self.root + "/storage/col2/TEST2", bso, status=400)
        # Before those expire, update ttl on one that exists
        # and on one that does not.
        time.sleep(0.2)
        bso = {"ttl": 10}
        self.app.post_json(self.root + "/storage/col2/TEST2", bso)
        self.app.post_json(self.root + "/storage/col2/TEST3", bso)
        # Update some other field on TEST1, which should leave ttl untouched.
        bso = {"sortindex": 3}
        self.app.post_json(self.root + "/storage/col2/TEST1", bso)
        # If we wait, TEST1 should expire but the others should not.
        time.sleep(0.8)
        items = self.app.get(self.root + "/storage/col2?full=1").json["items"]
        items = dict((item["id"], item) for item in items)
        self.assertEquals(sorted(items.keys()), ["TEST2", "TEST3"])
        # The existing item should have retained its payload.
        # The new item should have got a default payload of empty string.
        self.assertEquals(items["TEST2"]["payload"], "x")
        self.assertEquals(items["TEST3"]["payload"], "")

    def test_bulk_update_of_ttls_without_sending_data(self):
        # Create 5 BSOs with a ttl of 1 second.
        bsos = [{"id": str(i), "payload": "x", "ttl": 1} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)
        # Before they expire, bulk-update the ttl to something longer.
        # Also send data for some that don't exist yet.
        # And just to be really tricky, we're also going to update
        # one of the payloads at the same time.
        time.sleep(0.2)
        bsos = [{"id": str(i), "ttl": 10} for i in xrange(3, 7)]
        bsos[0]["payload"] = "xx"
        r = self.app.post_json(self.root + "/storage/col2", bsos)
        self.assertEquals(len(r.json["success"]), 4)
        # If we wait then items 0, 1, 2 should have expired.
        # Items 3, 4, 5, 6 should still exist.
        time.sleep(0.8)
        items = self.app.get(self.root + "/storage/col2?full=1").json["items"]
        items = dict((item["id"], item) for item in items)
        self.assertEquals(sorted(items.keys()), ["3", "4", "5", "6"])
        # Items 3 and 4 should have the specified payloads.
        # Items 5 and 6 should have payload defaulted to empty string.
        self.assertEquals(items["3"]["payload"], "xx")
        self.assertEquals(items["4"]["payload"], "x")
        self.assertEquals(items["5"]["payload"], "")
        self.assertEquals(items["6"]["payload"], "")

    def test_that_negative_integer_fields_are_not_accepted(self):
        # ttls cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {
            "payload": "TEST",
            "ttl": -1,
        }, status=400)
        # sortindex cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {
            "payload": "TEST",
            "sortindex": -42,
        }, status=400)
        # limit cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {"payload": "X"})
        self.app.get(self.root + "/storage/col2?limit=-1", status=400)
        # X-If-Modified-Since-Version cannot be negative
        self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since-Version": "-3",
        }, status=400)
        # X-If-Unmodified-Since-Version cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {
            "payload": "TEST",
        }, headers={
            "X-If-Unmodified-Since-Version": "-3",
        }, status=400)


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
        ver = int(res.headers["X-Last-Modified-Version"])

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        self.app.post_json(self.root + '/storage/tabs', bsos)

        # asking for bsos using newer=ts where newer is the timestamps
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/tabs?newer=%s' % ver)
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
