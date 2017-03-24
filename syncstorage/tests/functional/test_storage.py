# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Functional tests for the SyncStorage server protocol.

This file runs tests to ensure the correct operation of the server
as specified in:

    http://docs.services.mozilla.com/storage/apis-1.5.html

If there's an aspect of that spec that's not covered by a test in this file,
consider it a bug.

"""

import unittest2

import re
import sys
import time
import random
import string
import urllib
import webtest
import contextlib
# import math

from pyramid.interfaces import IAuthenticationPolicy

import tokenlib

from syncstorage.tests.functional.support import StorageFunctionalTestCase
from syncstorage.tests.functional.support import run_live_functional_tests
from syncstorage.util import json_loads, json_dumps
from syncstorage.tweens import WEAVE_INVALID_WBO, WEAVE_SIZE_LIMIT_EXCEEDED
from syncstorage.storage import ConflictError
from syncstorage.views.validators import BATCH_MAX_IDS
from syncstorage.views.util import get_limit_config

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
        self.root = '/1.5/%d' % (self.user_id,)
        # Reset the storage to a known state, aka "empty".
        self.app.delete(self.root)

    @contextlib.contextmanager
    def _switch_user(self):
        orig_root = self.root
        try:
            with super(TestStorage, self)._switch_user():
                self.root = '/1.5/%d' % (self.user_id,)
                yield
        finally:
            self.root = orig_root

    def test_get_info_collections(self):
        # col1 gets 3 items, col2 gets 5 items.
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(3)]
        resp = self.app.post_json(self.root + "/storage/col1", bsos)
        ts1 = resp.json["modified"]
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        resp = self.app.post_json(self.root + "/storage/col2", bsos)
        ts2 = resp.json["modified"]
        # only those collections should appear in the query.
        resp = self.app.get(self.root + '/info/collections')
        res = resp.json
        keys = sorted(res.keys())
        self.assertEquals(keys, ["col1", "col2"])
        self.assertEquals(res["col1"], ts1)
        self.assertEquals(res["col2"], ts2)
        # Updating items in col2, check timestamps.
        bsos = [{"id": str(i), "payload": "yyy"} for i in xrange(2)]
        resp = self.app.post_json(self.root + "/storage/col2", bsos)
        self.assertTrue(ts2 < resp.json["modified"])
        ts2 = resp.json["modified"]
        resp = self.app.get(self.root + '/info/collections')
        res = resp.json
        keys = sorted(res.keys())
        self.assertEquals(keys, ["col1", "col2"])
        self.assertEquals(res["col1"], ts1)
        self.assertEquals(res["col2"], ts2)

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

        # non-existent collections appear as empty
        resp = self.app.get(self.root + '/storage/nonexistent')
        res = resp.json
        self.assertEquals(res, [])

        # try just getting all items at once.
        resp = self.app.get(self.root + '/storage/col2')
        res = resp.json
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 5)

        # trying various filters

        # "ids"
        # Returns the ids for objects in the collection that are in the
        # provided comma-separated list.
        res = self.app.get(self.root + '/storage/col2?ids=1,3,17')
        res = res.json
        res.sort()
        self.assertEquals(res, ['1', '3'])

        # "newer"
        # Returns only ids for objects in the collection that have been last
        # modified after the timestamp given.

        self.app.delete(self.root + '/storage/col2')

        bso = {'id': '128', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/128', bso)
        ts1 = float(res.headers["X-Last-Modified"])

        bso = {'id': '129', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/129', bso)
        ts2 = float(res.headers["X-Last-Modified"])

        self.assertTrue(ts1 < ts2)

        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts1)
        self.assertEquals(res.json, ['129'])

        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts2)
        self.assertEquals(res.json, [])

        res = self.app.get(self.root + '/storage/col2?newer=%s' % (ts1 - 1))
        self.assertEquals(sorted(res.json), ['128', '129'])

        # "older"
        # Returns only ids for objects in the collection that have been last
        # modified before the timestamp given.

        self.app.delete(self.root + '/storage/col2')

        bso = {'id': '128', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/128', bso)
        ts1 = float(res.headers["X-Last-Modified"])

        bso = {'id': '129', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/129', bso)
        ts2 = float(res.headers["X-Last-Modified"])

        self.assertTrue(ts1 < ts2)

        res = self.app.get(self.root + '/storage/col2?older=%s' % ts1)
        self.assertEquals(res.json, [])

        res = self.app.get(self.root + '/storage/col2?older=%s' % ts2)
        self.assertEquals(res.json, ['128'])

        res = self.app.get(self.root + '/storage/col2?older=%s' % (ts2 + 1))
        self.assertEquals(sorted(res.json), ['128', '129'])

        qs = '?older=%s&newer=%s' % (ts2 + 1, ts1)
        res = self.app.get(self.root + '/storage/col2' + qs)
        self.assertEquals(sorted(res.json), ['129'])

        # "full"
        # If defined, returns the full BSO, rather than just the id.
        res = self.app.get(self.root + '/storage/col2?full=1')
        keys = res.json[0].keys()
        keys.sort()
        wanted = ['id', 'modified', 'payload']
        self.assertEquals(keys, wanted)

        res = self.app.get(self.root + '/storage/col2')
        self.assertTrue(isinstance(res.json, list))

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
        all_items = res.json
        self.assertEquals(len(all_items), 10)

        res = self.app.get(query_url + '&limit=2')
        self.assertEquals(res.json, all_items[:2])

        # "offset"
        # Skips over items that have already been returned.
        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&limit=3&offset=' + next_offset)
        self.assertEquals(res.json, all_items[2:5])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])
        self.assertTrue("X-Weave-Next-Offset" not in res.headers)

        res = self.app.get(query_url + '&limit=10000&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])
        self.assertTrue("X-Weave-Next-Offset" not in res.headers)

        # "offset" again, this time ordering by descending timestamp.
        query_url = self.root + '/storage/col2?sort=newest'
        res = self.app.get(query_url)
        all_items = res.json
        self.assertEquals(len(all_items), 10)

        res = self.app.get(query_url + '&limit=2')
        self.assertEquals(res.json, all_items[:2])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&limit=3&offset=' + next_offset)
        self.assertEquals(res.json, all_items[2:5])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])
        self.assertTrue("X-Weave-Next-Offset" not in res.headers)

        res = self.app.get(query_url + '&limit=10000&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])

        # "offset" again, this time ordering by ascending timestamp.
        query_url = self.root + '/storage/col2?sort=oldest'
        res = self.app.get(query_url)
        all_items = res.json
        self.assertEquals(len(all_items), 10)

        res = self.app.get(query_url + '&limit=2')
        self.assertEquals(res.json, all_items[:2])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&limit=3&offset=' + next_offset)
        self.assertEquals(res.json, all_items[2:5])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])
        self.assertTrue("X-Weave-Next-Offset" not in res.headers)

        res = self.app.get(query_url + '&limit=10000&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])

        # "offset" once more, this time with no explicit ordering
        query_url = self.root + '/storage/col2?'
        res = self.app.get(query_url)
        all_items = res.json
        self.assertEquals(len(all_items), 10)

        res = self.app.get(query_url + '&limit=2')
        self.assertEquals(res.json, all_items[:2])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&limit=3&offset=' + next_offset)
        self.assertEquals(res.json, all_items[2:5])

        next_offset = res.headers["X-Weave-Next-Offset"]
        res = self.app.get(query_url + '&offset=' + next_offset)
        self.assertEquals(res.json, all_items[5:])
        self.assertTrue("X-Weave-Next-Offset" not in res.headers)

        res = self.app.get(query_url + '&limit=10000&offset=' + next_offset)

        # "sort"
        #   'newest': Orders by timestamp number (newest first)
        #   'oldest': Orders by timestamp number (oldest first)
        #   'index':  Orders by the sortindex descending (highest weight first)
        self.app.delete(self.root + '/storage/col2')

        for index, sortindex in (('0', -1), ('1', 34), ('2', 12)):
            bso = {'id': index, 'payload': 'x', 'sortindex': sortindex}
            self.app.post_json(self.root + '/storage/col2', [bso])

        res = self.app.get(self.root + '/storage/col2?sort=newest')
        res = res.json
        self.assertEquals(res, ['2', '1', '0'])

        res = self.app.get(self.root + '/storage/col2?sort=oldest')
        res = res.json
        self.assertEquals(res, ['0', '1', '2'])

        res = self.app.get(self.root + '/storage/col2?sort=index')
        res = res.json
        self.assertEquals(res, ['1', '2', '0'])

    def test_alternative_formats(self):
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)

        # application/json
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/json')])
        self.assertEquals(res.content_type.split(";")[0], 'application/json')

        res = res.json
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

        # application/newlines
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/newlines')])
        self.assertEquals(res.content_type, 'application/newlines')

        self.assertTrue(res.body.endswith('\n'))
        res = [json_loads(line) for line in res.body.strip().split('\n')]
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
        res = self.app.get(self.root + '/storage/col2?full=true').json
        self.assertEquals(len(res), 5)
        timestamps = sorted([r["modified"] for r in res])
        # The timestamp of the collection should be the max of all those.
        self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since": str(timestamps[0])
        }, status=200)
        res = self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since": str(timestamps[-1])
        }, status=304)
        self.assertTrue("X-Last-Modified" in res.headers)

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
            "X-If-Modified-Since": str(res["modified"] + 1)
        }, status=304)
        res = self.app.get(self.root + '/storage/col2/1', headers={
            "X-If-Modified-Since": str(res["modified"] - 1)
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
        body = "\n".join(json_dumps(bso) for bso in bsos)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/newlines"
        })
        items = self.app.get(self.root + "/storage/col2").json
        self.assertEquals(len(items), 2)
        # If we send an unknown content type, we get an error.
        self.app.delete(self.root + "/storage/col2")
        body = json_dumps(bsos)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/octet-stream"
        }, status=415)
        items = self.app.get(self.root + "/storage/col2").json
        self.assertEquals(len(items), 0)

    def test_set_item_input_formats(self):
        # If we send with application/json it should work.
        body = json_dumps({'payload': _PLD})
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
        # Unless we use text/plain, which is a special bw-compat case.
        self.app.put(self.root + '/storage/col2/TEST', body, headers={
            "Content-Type": "text/plain"
        })
        item = self.app.get(self.root + "/storage/col2/TEST").json
        self.assertEquals(item["payload"], _PLD)

    def test_app_newlines_when_payloads_contain_newlines(self):
        # Send some application/newlines with embedded newline chars.
        bsos = [
            {'id': '1', 'payload': 'hello\nworld'},
            {'id': '2', 'payload': '\nmarco\npolo\n'},
        ]
        body = "\n".join(json_dumps(bso) for bso in bsos)
        self.assertEquals(len(body.split("\n")), 2)
        self.app.post(self.root + '/storage/col2', body, headers={
            "Content-Type": "application/newlines"
        })
        # Read them back as JSON list, check payloads.
        items = self.app.get(self.root + "/storage/col2?full=1").json
        self.assertEquals(len(items), 2)
        items.sort(key=lambda bso: bso["id"])
        self.assertEquals(items[0]["payload"], bsos[0]["payload"])
        self.assertEquals(items[1]["payload"], bsos[1]["payload"])
        # Read them back as application/newlines, check payloads.
        res = self.app.get(self.root + "/storage/col2?full=1", headers={
            "Accept": "application/newlines",
        })
        items = [json_loads(line) for line in res.body.strip().split('\n')]
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
        wanted = (len(bso1['payload']) + len(bso2['payload'])) / 1024.0
        self.assertEqual(round(col2_size, 2), round(wanted, 2))

    def test_delete_collection_items(self):
        # creating a collection of three
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bso3 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting all items
        self.app.delete(self.root + '/storage/col2')
        items = self.app.get(self.root + '/storage/col2').json
        self.assertEquals(len(items), 0)

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
        # creating a collection of three
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bso3 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)
        ts = float(res.headers['X-Last-Modified'])

        # deleting item 13
        self.app.delete(self.root + '/storage/col2/13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 2)

        # unexisting item should return a 404
        self.app.delete(self.root + '/storage/col2/12982', status=404)

        # The collection should get an updated timestsamp.
        res = self.app.get(self.root + '/info/collections')
        self.assertTrue(ts < float(res.headers['X-Last-Modified']))

    def test_delete_storage(self):
        # creating a collection of three
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bso3 = {'id': '14', 'payload': _PLD}
        bsos = [bso1, bso2, bso3]
        self.app.post_json(self.root + '/storage/col2', bsos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting all
        self.app.delete(self.root + '/storage')
        items = self.app.get(self.root + '/storage/col2').json
        self.assertEquals(len(items), 0)
        self.app.delete(self.root + '/storage/col2', status=200)
        self.assertEquals(len(items), 0)

    def test_x_timestamp_header(self):
        # This can't be run against a live server.
        if self.distant:
            raise unittest2.SkipTest

        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(5)]
        self.app.post_json(self.root + "/storage/col2", bsos)

        now = round(time.time(), 2)
        time.sleep(0.01)
        res = self.app.get(self.root + '/storage/col2')
        self.assertTrue(now < float(res.headers['X-Weave-Timestamp']))

        # getting the timestamp with a PUT
        now = round(time.time(), 2)
        time.sleep(0.01)
        bso = {'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        self.assertTrue(now < float(res.headers['X-Weave-Timestamp']))
        self.assertTrue(abs(now -
                        float(res.headers['X-Weave-Timestamp'])) < 200)

        # getting the timestamp with a POST
        now = round(time.time(), 2)
        time.sleep(0.01)
        bso1 = {'id': '12', 'payload': _PLD}
        bso2 = {'id': '13', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        self.assertTrue(now < float(res.headers['X-Weave-Timestamp']))

    def test_ifunmodifiedsince(self):
        bso = {'id': '12345', 'payload': _PLD}
        res = self.app.put_json(self.root + '/storage/col2/12345', bso)
        # Using an X-If-Unmodified-Since in the past should cause 412s.
        ts = str(float(res.headers['X-Last-Modified']) - 1)
        bso = {'id': '12345', 'payload': _PLD + "XXX"}
        res = self.app.put_json(
            self.root + '/storage/col2/12345', bso,
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        self.assertTrue("X-Last-Modified" in res.headers)
        res = self.app.delete(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        self.assertTrue("X-Last-Modified" in res.headers)
        self.app.post_json(
            self.root + '/storage/col2', [bso],
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        self.app.delete(
            self.root + '/storage/col2?ids=12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        self.app.get(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        self.app.get(
            self.root + '/storage/col2',
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        # Deleting items from a collection should give 412 even if some
        # other, unrelated item in the collection has been modified.
        ts = res.headers['X-Last-Modified']
        res2 = self.app.put_json(self.root + '/storage/col2/54321', {
            'payload': _PLD,
        })
        self.app.delete(
            self.root + '/storage/col2?ids=12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=412)
        ts = res2.headers['X-Last-Modified']
        # All of those should have left the BSO unchanged
        res2 = self.app.get(self.root + '/storage/col2/12345')
        self.assertEquals(res2.json['payload'], _PLD)
        self.assertEquals(res2.headers['X-Last-Modified'],
                          res.headers['X-Last-Modified'])
        # Using an X-If-Unmodified-Since equal to
        # X-Last-Modified should allow the request to succeed.
        res = self.app.post_json(
            self.root + '/storage/col2', [bso],
            headers=[('X-If-Unmodified-Since', ts)],
            status=200)
        ts = res.headers['X-Last-Modified']
        self.app.get(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=200)
        self.app.delete(
            self.root + '/storage/col2/12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=200)
        res = self.app.put_json(
            self.root + '/storage/col2/12345', bso,
            headers=[('X-If-Unmodified-Since', '0')],
            status=200)
        ts = res.headers['X-Last-Modified']
        self.app.get(
            self.root + '/storage/col2',
            headers=[('X-If-Unmodified-Since', ts)],
            status=200)
        self.app.delete(
            self.root + '/storage/col2?ids=12345',
            headers=[('X-If-Unmodified-Since', ts)],
            status=200)

    def test_quota(self):
        res = self.app.get(self.root + '/info/quota')
        old_used = res.json[0]
        bso = {'payload': _PLD}
        self.app.put_json(self.root + '/storage/col2/12345', bso)
        res = self.app.get(self.root + '/info/quota')
        used = res.json[0]
        self.assertEquals(used - old_used, len(_PLD) / 1024.0)

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
        wanted = str(round(200 / 1024.0, 2))
        self.assertEquals(res.headers['X-Weave-Quota-Remaining'], wanted)

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
        # This can't be run against a live server
        # due to request size limits in nginx and
        # the fact that it reads config variables.
        if self.distant:
            raise unittest2.SkipTest

        # Test that batch uploads are correctly processed.
        # Uploading max_count-5 small objects should succeed.
        max_count = get_limit_config(self.config, 'max_post_records')
        bsos = [{'id': str(i), 'payload': 'X'} for i in range(max_count - 5)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), max_count - 5)
        self.assertEquals(len(res['failed']), 0)

        # Uploading max_count+5 items should produce five failures.
        bsos = [{'id': str(i), 'payload': 'X'} for i in range(max_count + 5)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), max_count)
        self.assertEquals(len(res['failed']), 5)

        # The test config has max_bytes=1M.
        # Uploading 5 210MB items should produce one failure.
        max_bytes = get_limit_config(self.config, 'max_post_bytes')
        self.assertEquals(max_bytes, 1024 * 1024)
        bsos = [{'id': str(i), 'payload': "X" * (210 * 1024)}
                for i in range(5)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json
        self.assertEquals(len(res['success']), 4)
        self.assertEquals(len(res['failed']), 1)

    def test_weird_args(self):
        # pushing some data in col2
        bsos = [{'id': str(i), 'payload': _PLD} for i in range(10)]
        res = self.app.post_json(self.root + '/storage/col2', bsos)
        res = res.json

        # trying weird args and make sure the server returns 400s
        args = ('newer', 'older', 'limit', 'offset')
        for arg in args:
            value = randtext()
            self.app.get(self.root + '/storage/col2?%s=%s' % (arg, value),
                         status=400)

        # what about a crazy ids= string ?
        ids = ','.join([randtext(10) for i in range(100)])
        res = self.app.get(self.root + '/storage/col2?ids=%s' % ids)
        self.assertEquals(res.json, [])

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
        self.assertEqual(len(res.json), 0)
        res = self.app.get(self.root + '/storage/passwords')
        self.assertEqual(len(res.json), 3)

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
        self.assertEqual(len(res.json), 0)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEqual(len(res.json), 3)

    def test_timestamp_numbers_are_decimals(self):
        # Create five items with different timestamps.
        for i in xrange(5):
            bsos = [{"id": str(i), "payload": "xxx"}]
            self.app.post_json(self.root + "/storage/col2", bsos)

        # make sure the server returns only proper precision timestamps.
        resp = self.app.get(self.root + '/storage/col2?full=1')
        bsos = json_loads(resp.body)
        timestamps = []
        for bso in bsos:
            ts = bso['modified']
            self.assertEqual(len(str(ts).split(".")[-1]), 2)
            timestamps.append(ts)

        timestamps.sort()

        # try a newer filter now, to get the last two objects
        ts = float(timestamps[-3])

        # Returns only ids for objects in the collection that have been
        # last modified since the timestamp given.
        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts)
        res = res.json
        try:
            self.assertEquals(sorted(res), ['3', '4'])
        except AssertionError:
            # need to display the whole collection to understand the issue
            msg = 'Timestamp used: %s' % ts
            msg += ' ' + self.app.get(self.root + '/storage/col2?full=1').body
            msg += ' Timestamps received: %s' % str(timestamps)
            raise AssertionError(msg)

    def test_strict_newer(self):
        # send two bsos in the 'meh' collection
        bso1 = {'id': '1', 'payload': _PLD}
        bso2 = {'id': '2', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/meh', bsos)
        ts = float(res.headers["X-Last-Modified"])

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        res = self.app.post_json(self.root + '/storage/meh', bsos)

        # asking for bsos using newer=ts where newer is the timestamp
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/meh?newer=%s' % ts)
        res = res.json
        self.assertEquals(sorted(res), ['3', '4'])

    def test_strict_older(self):
        # send two bsos in the 'meh' collection
        bso1 = {'id': '1', 'payload': _PLD}
        bso2 = {'id': '2', 'payload': _PLD}
        bsos = [bso1, bso2]
        res = self.app.post_json(self.root + '/storage/meh', bsos)

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        res = self.app.post_json(self.root + '/storage/meh', bsos)
        ts = float(res.headers["X-Last-Modified"])

        # asking for bsos using older=ts where older is the timestamp
        # of bso 3 and 4, should not return them
        res = self.app.get(self.root + '/storage/meh?older=%s' % ts)
        res = res.json
        self.assertEquals(sorted(res), ['1', '2'])

    def test_handling_of_invalid_json_in_bso_uploads(self):
        # Single upload with JSON that's not a BSO.
        bso = "notabso"
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                                status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)

        bso = 42
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                                status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)

        bso = {'id': ["1", "2"], 'payload': {'3': '4'}}
        res = self.app.put_json(self.root + '/storage/col2/invalid', bso,
                                status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)

        # Batch upload with JSON that's not a list of BSOs
        bsos = "notalist"
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)

        bsos = 42
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)

        # Batch upload a list with something that's not a valid data dict.
        # It should fail out entirely, as the input is seriously broken.
        bsos = [{'id': '1', 'payload': 'GOOD'}, "BAD"]
        res = self.app.post_json(self.root + '/storage/col2', bsos, status=400)

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
        # The newline cases are especially nuanced because \n
        # gets special treatment from the regex library.
        bso = {"id": "A\nB", "payload": "testing"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        bso = {"id": "A\n", "payload": "testing"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        bso = {"id": "\nN", "payload": "testing"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        bso = {"id": "A\tB", "payload": "testing"}
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
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=404)
        # Invalid sortindex - not an integer
        bso = {"id": "TEST", "payload": "testing", "sortindex": "meh"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)
        # Invalid sortindex - not an integer
        bso = {"id": "TEST", "payload": "testing", "sortindex": "2.6"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)
        # Invalid sortindex - larger than max value
        bso = {"id": "TEST", "payload": "testing", "sortindex": "1" + "0" * 9}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)
        # Invalid payload - not a string
        bso = {"id": "TEST", "payload": 42}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)
        # Invalid ttl - not an integer
        bso = {"id": "TEST", "payload": "testing", "ttl": "eh?"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)
        # Invalid ttl - not an integer
        bso = {"id": "TEST", "payload": "testing", "ttl": "4.2"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)
        # Invalid BSO - unknown field
        bso = {"id": "TEST", "unexpected": "spanish-inquisition"}
        res = self.app.post_json(coll_url, [bso])
        self.assertTrue(res.json["failed"] and not res.json["success"])
        res = self.app.put_json(coll_url + "/" + bso["id"], bso, status=400)
        self.assertEquals(res.json, WEAVE_INVALID_WBO)

    def test_that_batch_gets_are_limited_to_max_number_of_ids(self):
        bso = {"id": "1", "payload": "testing"}
        self.app.put_json(self.root + "/storage/col2/1", bso)

        # Getting with less than the limit works OK.
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_IDS - 1))
        res = self.app.get(self.root + "/storage/col2?ids=" + ids)
        self.assertEquals(res.json, ["1"])

        # Getting with equal to the limit works OK.
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_IDS))
        res = self.app.get(self.root + "/storage/col2?ids=" + ids)
        self.assertEquals(res.json, ["1"])

        # Getting with more than the limit fails.
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_IDS + 1))
        self.app.get(self.root + "/storage/col2?ids=" + ids, status=400)

    def test_that_batch_deletes_are_limited_to_max_number_of_ids(self):
        bso = {"id": "1", "payload": "testing"}

        # Deleting with less than the limit works OK.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_IDS - 1))
        self.app.delete(self.root + "/storage/col2?ids=" + ids)

        # Deleting with equal to the limit works OK.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_IDS))
        self.app.delete(self.root + "/storage/col2?ids=" + ids)

        # Deleting with more than the limit fails.
        self.app.put_json(self.root + "/storage/col2/1", bso)
        ids = ",".join(str(i) for i in xrange(BATCH_MAX_IDS + 1))
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
        ts1 = float(r.headers["X-Last-Modified"])
        self.assertTrue(ts1 > 0)
        # With X-I-M-S set before latest change, all should give a 200.
        headers = {"X-If-Modified-Since": str(ts1 - 1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=200)
        # With X-I-M-S set to after latest change , all should give a 304.
        headers = {"X-If-Modified-Since": str(ts1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=304)
        # Change a collection.
        bso = {"payload": "TEST"}
        r = self.app.put_json(self.root + "/storage/col2/TEST", bso)
        ts2 = r.headers["X-Last-Modified"]
        # Using the previous version should read the updated data.
        headers = {"X-If-Modified-Since": str(ts1)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=200)
        # Using the new timestamp should produce 304s.
        headers = {"X-If-Modified-Since": str(ts2)}
        for view in INFO_VIEWS:
            self.app.get(self.root + view, headers=headers, status=304)
        # XXX TODO: the storage-level timestamp is not tracked correctly
        # after deleting a collection, so this test fails for now.
        # # Delete a collection.
        # r = self.app.delete(self.root + "/storage/col2")
        # ts3 = r.headers["X-Last-Modified"]
        # # Using the previous timestamp should read the updated data.
        # headers = {"X-If-Modified-Since": str(ts2)}
        # for view in INFO_VIEWS:
        #     self.app.get(self.root + view, headers=headers, status=200)
        # # Using the new timestamp should produce 304s.
        # headers = {"X-If-Modified-Since": str(ts3)}
        # for view in INFO_VIEWS:
        #     self.app.get(self.root + view, headers=headers, status=304)

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

    def test_update_of_ttl_without_sending_data(self):
        bso = {"payload": "x", "ttl": 1}
        self.app.put_json(self.root + "/storage/col2/TEST1", bso)
        self.app.put_json(self.root + "/storage/col2/TEST2", bso)
        # Before those expire, update ttl on one that exists
        # and on one that does not.
        time.sleep(0.2)
        bso = {"ttl": 10}
        self.app.put_json(self.root + "/storage/col2/TEST2", bso)
        self.app.put_json(self.root + "/storage/col2/TEST3", bso)
        # Update some other field on TEST1, which should leave ttl untouched.
        bso = {"sortindex": 3}
        self.app.put_json(self.root + "/storage/col2/TEST1", bso)
        # If we wait, TEST1 should expire but the others should not.
        time.sleep(0.8)
        items = self.app.get(self.root + "/storage/col2?full=1").json
        items = dict((item["id"], item) for item in items)
        self.assertEquals(sorted(items.keys()), ["TEST2", "TEST3"])
        # The existing item should have retained its payload.
        # The new item should have got a default payload of empty string.
        self.assertEquals(items["TEST2"]["payload"], "x")
        self.assertEquals(items["TEST3"]["payload"], "")
        ts2 = items["TEST2"]["modified"]
        ts3 = items["TEST3"]["modified"]
        self.assertTrue(ts2 < ts3)

    def test_bulk_update_of_ttls_without_sending_data(self):
        # Create 5 BSOs with a ttl of 1 second.
        bsos = [{"id": str(i), "payload": "x", "ttl": 1} for i in xrange(5)]
        r = self.app.post_json(self.root + "/storage/col2", bsos)
        ts1 = float(r.headers["X-Last-Modified"])
        # Before they expire, bulk-update the ttl to something longer.
        # Also send data for some that don't exist yet.
        # And just to be really tricky, we're also going to update
        # one of the payloads at the same time.
        time.sleep(0.2)
        bsos = [{"id": str(i), "ttl": 10} for i in xrange(3, 7)]
        bsos[0]["payload"] = "xx"
        r = self.app.post_json(self.root + "/storage/col2", bsos)
        self.assertEquals(len(r.json["success"]), 4)
        ts2 = float(r.headers["X-Last-Modified"])
        # If we wait then items 0, 1, 2 should have expired.
        # Items 3, 4, 5, 6 should still exist.
        time.sleep(0.8)
        items = self.app.get(self.root + "/storage/col2?full=1").json
        items = dict((item["id"], item) for item in items)
        self.assertEquals(sorted(items.keys()), ["3", "4", "5", "6"])
        # Items 3 and 4 should have the specified payloads.
        # Items 5 and 6 should have payload defaulted to empty string.
        self.assertEquals(items["3"]["payload"], "xx")
        self.assertEquals(items["4"]["payload"], "x")
        self.assertEquals(items["5"]["payload"], "")
        self.assertEquals(items["6"]["payload"], "")
        # All items created or modified by the request should get their
        # timestamps update.  Just bumping the ttl should not bump timestamp.
        self.assertEquals(items["3"]["modified"], ts2)
        self.assertEquals(items["4"]["modified"], ts1)
        self.assertEquals(items["5"]["modified"], ts2)
        self.assertEquals(items["6"]["modified"], ts2)

    def test_that_negative_integer_fields_are_not_accepted(self):
        # ttls cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {
            "payload": "TEST",
            "ttl": -1,
        }, status=400)
        # limit cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {"payload": "X"})
        self.app.get(self.root + "/storage/col2?limit=-1", status=400)
        # X-If-Modified-Since cannot be negative
        self.app.get(self.root + "/storage/col2", headers={
            "X-If-Modified-Since": "-3",
        }, status=400)
        # X-If-Unmodified-Since cannot be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {
            "payload": "TEST",
        }, headers={
            "X-If-Unmodified-Since": "-3",
        }, status=400)
        # sortindex actually *can* be negative
        self.app.put_json(self.root + "/storage/col2/TEST", {
            "payload": "TEST",
            "sortindex": -42,
        }, status=200)

    def test_meta_global_sanity(self):
        # Memcache backend is configured to store 'meta' in write-through
        # cache, so we want to check it explicitly.  We might as well put it
        # in the base tests because there's nothing memcached-specific here.
        self.app.get(self.root + '/storage/meta/global', status=404)
        res = self.app.get(self.root + '/storage/meta')
        self.assertEquals(res.json, [])
        self.app.put_json(self.root + '/storage/meta/global',
                          {'payload': 'blob'})
        res = self.app.get(self.root + '/storage/meta')
        self.assertEquals(res.json, ['global'])
        res = self.app.get(self.root + '/storage/meta/global')
        self.assertEquals(res.json['payload'], 'blob')
        # It should not have extra keys.
        keys = res.json.keys()
        keys.sort()
        self.assertEquals(keys, ['id', 'modified', 'payload'])
        # It should have a properly-formatted "modified" field.
        modified_re = r"['\"]modified['\"]:\s*[0-9]+\.[0-9][0-9]\s*[,}]"
        self.assertTrue(re.search(modified_re, res.body))
        # Any client-specified "modified" field should be ignored
        res = self.app.put_json(self.root + '/storage/meta/global',
                                {'payload': 'blob', 'modified': 12})
        ts = float(res.headers['X-Weave-Timestamp'])
        res = self.app.get(self.root + '/storage/meta/global')
        self.assertEquals(res.json['modified'], ts)

    def test_that_404_responses_have_a_json_body(self):
        res = self.app.get(self.root + '/nonexistent/url', status=404)
        self.assertEquals(res.content_type, "application/json")
        self.assertEquals(res.json, 0)

    def test_that_internal_server_fields_are_not_echoed(self):
        self.app.post_json(self.root + '/storage/col1',
                           [{'id': 'one', 'payload': 'blob'}])
        self.app.put_json(self.root + '/storage/col1/two',
                          {'payload': 'blub'})
        res = self.app.get(self.root + '/storage/col1?full=1')
        self.assertEquals(len(res.json), 2)
        for item in res.json:
            self.assertTrue("id" in item)
            self.assertTrue("payload" in item)
            self.assertFalse("payload_size" in item)
            self.assertFalse("ttl" in item)
        for id in ('one', 'two'):
            res = self.app.get(self.root + '/storage/col1/' + id)
            self.assertTrue("id" in res.json)
            self.assertTrue("payload" in res.json)
            self.assertFalse("payload_size" in res.json)
            self.assertFalse("ttl" in res.json)

    def test_accessing_info_collections_with_an_expired_token(self):
        # This can't be run against a live server because we
        # have to forge an auth token to test things properly.
        if self.distant:
            raise unittest2.SkipTest

        # Write some items while we've got a good token.
        bsos = [{"id": str(i), "payload": "xxx"} for i in xrange(3)]
        resp = self.app.post_json(self.root + "/storage/col1", bsos)
        ts = float(resp.headers["X-Last-Modified"])

        # Check that we can read the info correctly.
        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(resp.json.keys(), ["col1"])
        self.assertEquals(resp.json["col1"], ts)

        # Forge an expired token to use for the test.
        auth_policy = self.config.registry.getUtility(IAuthenticationPolicy)
        secret = auth_policy._get_token_secrets(self.host_url)[-1]
        tm = tokenlib.TokenManager(secret=secret)
        exp = time.time() - 60
        data = {"uid": self.user_id, "node": self.host_url, "expires": exp}
        self.auth_token = tm.make_token(data)
        self.auth_secret = tm.get_derived_secret(self.auth_token)

        # The expired token cannot be used for normal operations.
        bsos = [{"id": str(i), "payload": "aaa"} for i in xrange(3)]
        self.app.post_json(self.root + "/storage/col1", bsos, status=401)
        self.app.get(self.root + "/storage/col1", status=401)

        # But it still allows access to /info/collections.
        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(resp.json.keys(), ["col1"])
        self.assertEquals(resp.json["col1"], ts)

    def test_pagination_with_newer_and_sort_by_oldest(self):
        # Twelve bsos with three different modification times.
        NUM_ITEMS = 12
        bsos = []
        timestamps = []
        for i in range(NUM_ITEMS):
            bso = {'id': str(i), 'payload': 'x'}
            bsos.append(bso)
            if i % 4 == 3:
                res = self.app.post_json(self.root + '/storage/col2', bsos)
                ts = float(res.headers["X-Last-Modified"])
                timestamps.append((i, ts))
                bsos = []

        # Try with several different pagination sizes,
        # to hit various boundary conditions.
        for limit in (2, 3, 4, 5, 6):
            for (start, ts) in timestamps:
                query_url = self.root + '/storage/col2?full=true&sort=oldest'
                query_url += '&newer=%s&limit=%s' % (ts, limit)

                # Paginated-ly fetch all items.
                items = []
                res = self.app.get(query_url)
                for item in res.json:
                    if items:
                        assert items[-1]['modified'] <= item['modified']
                    items.append(item)
                next_offset = res.headers.get('X-Weave-Next-Offset')
                while next_offset is not None:
                    res = self.app.get(query_url + "&offset=" + next_offset)
                    for item in res.json:
                        assert items[-1]['modified'] <= item['modified']
                        items.append(item)
                    next_offset = res.headers.get('X-Weave-Next-Offset')

                # They should all be in order, starting from the item
                # *after* the one that was used for the newer= timestamp.
                self.assertEquals(sorted(int(item['id']) for item in items),
                                  range(start + 1, NUM_ITEMS))

    def test_pagination_with_older_and_sort_by_newest(self):
        # Twelve bsos with three different modification times.
        NUM_ITEMS = 12
        bsos = []
        timestamps = []
        for i in range(NUM_ITEMS):
            bso = {'id': str(i), 'payload': 'x'}
            bsos.append(bso)
            if i % 4 == 3:
                res = self.app.post_json(self.root + '/storage/col2', bsos)
                ts = float(res.headers["X-Last-Modified"])
                timestamps.append((i - 3, ts))
                bsos = []

        # Try with several different pagination sizes,
        # to hit various boundary conditions.
        for limit in (2, 3, 4, 5, 6):
            for (start, ts) in timestamps:
                query_url = self.root + '/storage/col2?full=true&sort=newest'
                query_url += '&older=%s&limit=%s' % (ts, limit)

                # Paginated-ly fetch all items.
                items = []
                res = self.app.get(query_url)
                for item in res.json:
                    if items:
                        assert items[-1]['modified'] >= item['modified']
                    items.append(item)
                next_offset = res.headers.get('X-Weave-Next-Offset')
                while next_offset is not None:
                    res = self.app.get(query_url + "&offset=" + next_offset)
                    for item in res.json:
                        assert items[-1]['modified'] >= item['modified']
                        items.append(item)
                    next_offset = res.headers.get('X-Weave-Next-Offset')

                # They should all be in order, up to the item *before*
                # the one that was used for the older= timestamp.
                self.assertEquals(sorted(int(item['id']) for item in items),
                                  range(0, start))

    def assertCloseEnough(self, val1, val2, delta=0.05):
        if abs(val1 - val2) < delta:
            return True
        raise AssertionError("abs(%.2f - %.2f) = %.2f > %.2f"
                             % (val1, val2, abs(val1 - val2), delta))

    def test_batches(self):

        endpoint = self.root + '/storage/col2'

        bso1 = {'id': '12', 'payload': 'elegance'}
        bso2 = {'id': '13', 'payload': 'slovenly'}
        bsos = [bso1, bso2]
        self.app.post_json(endpoint, bsos)

        resp = self.app.get(endpoint + '/12')
        orig_modified = resp.headers['X-Last-Modified']

        bso3 = {'id': 'a', 'payload': 'internal'}
        bso4 = {'id': 'b', 'payload': 'pancreas'}
        resp = self.app.post_json(endpoint + '?batch=true', [bso3, bso4])
        batch = resp.json["batch"]

        # The collection should not be reported as modified.
        self.assertEquals(orig_modified, resp.headers['X-Last-Modified'])

        # And reading from it shouldn't show the new records yet.
        resp = self.app.get(endpoint)
        res = resp.json
        res.sort()
        self.assertEquals(res, ['12', '13'])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 2)
        self.assertEquals(orig_modified, resp.headers['X-Last-Modified'])

        bso5 = {'id': 'c', 'payload': 'tinsel'}
        bso6 = {'id': '13', 'payload': 'portnoy'}
        bso0 = {'id': '14', 'payload': 'itsybitsy'}
        commit = '?batch={0}&commit=true'.format(batch)
        resp = self.app.post_json(endpoint + commit, [bso5, bso6, bso0])
        committed = resp.json['modified']
        self.assertEquals(resp.json['modified'],
                          float(resp.headers['X-Last-Modified']))

        # make sure /info/collections got updated
        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(float(resp.headers['X-Last-Modified']), committed)
        self.assertEquals(resp.json['col2'], committed)

        # make sure the changes applied
        resp = self.app.get(endpoint)
        res = resp.json
        res.sort()
        self.assertEquals(res, ['12', '13', '14', 'a', 'b', 'c'])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 6)
        resp = self.app.get(endpoint + '/13')
        self.assertEquals(resp.json['payload'], 'portnoy')
        self.assertEquals(committed, float(resp.headers['X-Last-Modified']))
        self.assertEquals(committed, resp.json['modified'])
        resp = self.app.get(endpoint + '/c')
        self.assertEquals(resp.json['payload'], 'tinsel')
        self.assertEquals(committed, resp.json['modified'])
        resp = self.app.get(endpoint + '/14')
        self.assertEquals(resp.json['payload'], 'itsybitsy')
        self.assertEquals(committed, resp.json['modified'])

        # empty commit POST
        bso7 = {'id': 'a', 'payload': 'burrito'}
        bso8 = {'id': 'e', 'payload': 'chocolate'}
        resp = self.app.post_json(endpoint + '?batch=true', [bso7, bso8])
        batch = resp.json["batch"]
        time.sleep(1)
        commit = '?batch={0}&commit=true'.format(batch)

        resp1 = self.app.post_json(endpoint + commit, [])
        committed = resp1.json['modified']
        self.assertEquals(committed, float(resp1.headers['X-Last-Modified']))

        resp2 = self.app.get(endpoint + '/a')
        self.assertEquals(committed, float(resp2.headers['X-Last-Modified']))
        self.assertEquals(committed, resp2.json['modified'])
        self.assertEquals(resp2.json['payload'], 'burrito')

        resp3 = self.app.get(endpoint + '/e')
        self.assertEquals(committed, resp3.json['modified'])

    def test_we_dont_need_no_stinkin_batches(self):
        endpoint = self.root + '/storage/col2'

        # invalid batch ID
        bso1 = {'id': 'f', 'payload': 'pantomime'}
        self.app.post_json(endpoint + '?batch=sammich', [bso1], status=400)

        # commit with no batch ID
        self.app.post_json(endpoint + '?commit=true', [], status=400)

    def test_batch_size_limits(self):
        limits = self.app.get(self.root + '/info/configuration').json
        self.assertTrue('max_post_records' in limits)
        self.assertTrue('max_post_bytes' in limits)
        self.assertTrue('max_total_records' in limits)
        self.assertTrue('max_total_bytes' in limits)
        self.assertTrue('max_record_payload_bytes' in limits)

        endpoint = self.root + '/storage/col2?batch=true'

        res = self.app.post_json(endpoint, [], headers={
          'X-Weave-Records': str(limits['max_post_records'] + 1)
        }, status=400)
        self.assertEquals(res.json, WEAVE_SIZE_LIMIT_EXCEEDED)

        res = self.app.post_json(endpoint, [], headers={
          'X-Weave-Total-Records': str(limits['max_total_records'] + 1)
        }, status=400)
        self.assertEquals(res.json, WEAVE_SIZE_LIMIT_EXCEEDED)

        res = self.app.post_json(endpoint, [], headers={
          'X-Weave-Bytes': str(limits['max_post_bytes'] + 1)
        }, status=400)
        self.assertEquals(res.json, WEAVE_SIZE_LIMIT_EXCEEDED)

        res = self.app.post_json(endpoint, [], headers={
          'X-Weave-Total-Bytes': str(limits['max_total_bytes'] + 1)
        }, status=400)
        self.assertEquals(res.json, WEAVE_SIZE_LIMIT_EXCEEDED)

    def test_batch_partial_update(self):
        collection = self.root + '/storage/col2'
        bsos = [
          {'id': 'a', 'payload': 'aai'},
          {'id': 'b', 'payload': 'bee', 'sortindex': 17}
        ]
        resp = self.app.post_json(collection, bsos)
        orig_ts = float(resp.headers['X-Last-Modified'])

        # Update one, and add a new one.
        bsos = [
          {'id': 'b', 'payload': 'bii'},
          {'id': 'c', 'payload': 'sea'},
        ]
        resp = self.app.post_json(collection + '?batch=true', bsos)
        batch = resp.json["batch"]
        self.assertEquals(orig_ts, float(resp.headers['X-Last-Modified']))

        # The updated item hasn't been written yet.
        resp = self.app.get(collection + '?full=1')
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 2)
        self.assertEquals(res[0]['payload'], 'aai')
        self.assertEquals(res[1]['payload'], 'bee')
        self.assertEquals(res[0]['modified'], orig_ts)
        self.assertEquals(res[1]['modified'], orig_ts)
        self.assertEquals(res[1]['sortindex'], 17)

        endpoint = collection + '?batch={0}&commit=true'.format(batch)
        resp = self.app.post_json(endpoint, [])
        commit_ts = float(resp.headers['X-Last-Modified'])

        # The changes have now been applied.
        resp = self.app.get(collection + '?full=1')
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 3)
        self.assertEquals(res[0]['payload'], 'aai')
        self.assertEquals(res[1]['payload'], 'bii')
        self.assertEquals(res[2]['payload'], 'sea')
        self.assertEquals(res[0]['modified'], orig_ts)
        self.assertEquals(res[1]['modified'], commit_ts)
        self.assertEquals(res[2]['modified'], commit_ts)

        # Fields not touched by the batch, should have been preserved.
        self.assertEquals(res[1]['sortindex'], 17)

    def test_batch_ttl_update(self):
        collection = self.root + '/storage/col2'
        bsos = [
          {'id': 'a', 'payload': 'ayy'},
          {'id': 'b', 'payload': 'bea'},
          {'id': 'c', 'payload': 'see'}
        ]
        resp = self.app.post_json(collection, bsos)

        # Bump ttls as a series of individual batch operations.
        resp = self.app.post_json(collection + '?batch=true', [], status=202)
        orig_ts = float(resp.headers['X-Last-Modified'])
        batch = resp.json["batch"]

        endpoint = collection + '?batch={0}'.format(batch)
        resp = self.app.post_json(endpoint, [{'id': 'a', 'ttl': 2}],
                                  status=202)
        self.assertEquals(orig_ts, float(resp.headers['X-Last-Modified']))
        resp = self.app.post_json(endpoint, [{'id': 'b', 'ttl': 2}],
                                  status=202)
        self.assertEquals(orig_ts, float(resp.headers['X-Last-Modified']))
        resp = self.app.post_json(endpoint + '&commit=true', [],
                                  status=200)

        # The payloads should be unchanged
        resp = self.app.get(collection + '?full=1')
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 3)
        self.assertEquals(res[0]['payload'], 'ayy')
        self.assertEquals(res[1]['payload'], 'bea')
        self.assertEquals(res[2]['payload'], 'see')

        # If we wait, the ttls should kick in
        time.sleep(2.1)
        resp = self.app.get(collection + '?full=1')
        res = resp.json
        self.assertEquals(len(res), 1)
        self.assertEquals(res[0]['payload'], 'see')

    def test_batch_ttl_is_based_on_commit_timestamp(self):
        collection = self.root + '/storage/col2'

        resp = self.app.post_json(collection + '?batch=true', [], status=202)
        batch = resp.json["batch"]
        endpoint = collection + '?batch={0}'.format(batch)
        resp = self.app.post_json(endpoint, [{'id': 'a', 'ttl': 3}],
                                  status=202)

        # Put some time between upload timestamp and commit timestamp.
        time.sleep(1.5)

        resp = self.app.post_json(endpoint + '&commit=true', [],
                                  status=200)

        # Wait a little; if ttl is taken from the time of the commit
        # then it should not kick in just yet.
        time.sleep(1.6)
        resp = self.app.get(collection)
        res = resp.json
        self.assertEquals(len(res), 1)
        self.assertEquals(res[0], 'a')

        # Wait some more, and the ttl should kick in.
        time.sleep(1.6)
        resp = self.app.get(collection)
        res = resp.json
        self.assertEquals(len(res), 0)

    def test_batch_with_immediate_commit(self):
        collection = self.root + '/storage/col2'
        bsos = [
          {'id': 'a', 'payload': 'aih'},
          {'id': 'b', 'payload': 'bie'},
          {'id': 'c', 'payload': 'cee'}
        ]

        resp = self.app.post_json(collection + '?batch=true&commit=true', bsos,
                                  status=200)
        self.assertTrue('batch' not in resp.json)
        self.assertTrue('modified' in resp.json)
        committed = resp.json['modified']

        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(float(resp.headers['X-Last-Modified']), committed)
        self.assertEquals(resp.json['col2'], committed)

        resp = self.app.get(collection + '?full=1')
        self.assertEquals(float(resp.headers['X-Last-Modified']), committed)
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 3)
        self.assertEquals(res[0]['payload'], 'aih')
        self.assertEquals(res[1]['payload'], 'bie')
        self.assertEquals(res[2]['payload'], 'cee')

    def test_batch_uploads_properly_update_info_collections(self):
        collection1 = self.root + '/storage/col1'
        collection2 = self.root + '/storage/col2'
        bsos = [
          {'id': 'a', 'payload': 'aih'},
          {'id': 'b', 'payload': 'bie'},
          {'id': 'c', 'payload': 'cee'}
        ]

        resp = self.app.post_json(collection1, bsos)
        ts1 = resp.json['modified']

        resp = self.app.post_json(collection2, bsos)
        ts2 = resp.json['modified']

        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(float(resp.headers['X-Last-Modified']), ts2)
        self.assertEquals(resp.json['col1'], ts1)
        self.assertEquals(resp.json['col2'], ts2)

        # Overwrite in place, timestamp should change.
        resp = self.app.post_json(collection2 + '?batch=true&commit=true',
                                  bsos[:2])
        self.assertTrue(resp.json['modified'] > ts2)
        ts2 = resp.json['modified']

        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(float(resp.headers['X-Last-Modified']), ts2)
        self.assertEquals(resp.json['col1'], ts1)
        self.assertEquals(resp.json['col2'], ts2)

        # Add new items, timestamp should change
        resp = self.app.post_json(collection1 + '?batch=true&commit=true',
                                  [{'id': 'd', 'payload': 'dee'}])
        self.assertTrue(resp.json['modified'] > ts1)
        self.assertTrue(resp.json['modified'] >= ts2)
        ts1 = resp.json['modified']

        resp = self.app.get(self.root + '/info/collections')
        self.assertEquals(float(resp.headers['X-Last-Modified']), ts1)
        self.assertEquals(resp.json['col1'], ts1)
        self.assertEquals(resp.json['col2'], ts2)

    def test_batch_with_failing_bsos(self):
        collection = self.root + '/storage/col2'
        bsos = [
          {'id': 'a', 'payload': 'aai'},
          {'id': 'b\n', 'payload': 'i am invalid', 'sortindex': 17}
        ]
        resp = self.app.post_json(collection + '?batch=true', bsos)
        self.assertEqual(len(resp.json['failed']), 1)
        self.assertEqual(len(resp.json['success']), 1)
        batch = resp.json["batch"]

        bsos = [
          {'id': 'c', 'payload': 'sea'},
          {'id': 'd', 'payload': 'dii', 'ttl': -12},
        ]
        endpoint = collection + '?batch={0}&commit=true'.format(batch)
        resp = self.app.post_json(endpoint, bsos)
        self.assertEqual(len(resp.json['failed']), 1)
        self.assertEqual(len(resp.json['success']), 1)

        # To correctly match semantics of batchless POST, the batch
        # should be committed including only the successful items.
        # It is the client's responsibility to detect that some items
        # failed, and decide whether to commit the batch.
        resp = self.app.get(collection + '?full=1')
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 2)
        self.assertEquals(res[0]['payload'], 'aai')
        self.assertEquals(res[1]['payload'], 'sea')

    def test_batch_id_is_correctly_scoped_to_a_collection(self):
        collection1 = self.root + '/storage/col1'
        bsos = [
          {'id': 'a', 'payload': 'aih'},
          {'id': 'b', 'payload': 'bie'},
          {'id': 'c', 'payload': 'cee'}
        ]
        resp = self.app.post_json(collection1 + '?batch=true', bsos)
        batch = resp.json['batch']

        # I should not be able to add to that batch in a different collection.
        endpoint2 = self.root + '/storage/col2?batch={0}'.format(batch)
        resp = self.app.post_json(endpoint2, [{'id': 'd', 'payload': 'dii'}],
                                  status=400)

        # I should not be able to commit that batch in a different collection.
        resp = self.app.post_json(endpoint2 + '&commit=true', [],
                                  status=400)

        # I should still be able to use the batch in the correct collection.
        endpoint1 = collection1 + '?batch={0}'.format(batch)
        resp = self.app.post_json(endpoint1, [{'id': 'd', 'payload': 'dii'}])
        resp = self.app.post_json(endpoint1 + '&commit=true', [])

        resp = self.app.get(collection1 + '?full=1')
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 4)
        self.assertEquals(res[0]['payload'], 'aih')
        self.assertEquals(res[1]['payload'], 'bie')
        self.assertEquals(res[2]['payload'], 'cee')
        self.assertEquals(res[3]['payload'], 'dii')

    def test_batch_id_is_correctly_scoped_to_a_user(self):
        collection = self.root + '/storage/col1'
        bsos = [
          {'id': 'a', 'payload': 'aih'},
          {'id': 'b', 'payload': 'bie'},
          {'id': 'c', 'payload': 'cee'}
        ]
        resp = self.app.post_json(collection + '?batch=true', bsos)
        batch = resp.json['batch']

        with self._switch_user():
            # I should not be able to add to that batch as a different user.
            endpoint = self.root + '/storage/col1?batch={0}'.format(batch)
            resp = self.app.post_json(endpoint, [{'id': 'd', 'payload': 'di'}],
                                      status=400)

            # I should not be able to commit that batch as a different user.
            resp = self.app.post_json(endpoint + '&commit=true', [],
                                      status=400)

        # I should still be able to use the batch in the original user.
        endpoint = collection + '?batch={0}'.format(batch)
        resp = self.app.post_json(endpoint, [{'id': 'd', 'payload': 'di'}])
        resp = self.app.post_json(endpoint + '&commit=true', [])

        resp = self.app.get(collection + '?full=1')
        res = resp.json
        res.sort(key=lambda bso: bso['id'])
        self.assertEquals(len(res), 4)
        self.assertEquals(res[0]['payload'], 'aih')
        self.assertEquals(res[1]['payload'], 'bie')
        self.assertEquals(res[2]['payload'], 'cee')
        self.assertEquals(res[3]['payload'], 'di')

    # bug 1332552 make sure ttl:null use the default ttl
    def test_create_bso_with_null_ttl(self):
        bso = {"payload": "x", "ttl": None}
        self.app.put_json(self.root + "/storage/col2/TEST1", bso)
        time.sleep(0.1)
        res = self.app.get(self.root + "/storage/col2/TEST1?full=1")
        self.assertEquals(res.json["payload"], "x")

    def test_rejection_of_known_bad_payloads(self):
        bso = {
            "payload": json_dumps({
                "ciphertext": "IDontKnowWhatImDoing",
                "IV": "AAAAAAAAAAAAAAAAAAAAAA==",
            })
        }
        self.app.put_json(self.root + "/storage/col2/TEST1", bso, status=400)
        self.app.post_json(self.root + "/storage/col2", [bso], status=400)


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
        ts1 = float(res.headers["X-Last-Modified"])

        # send two more bsos
        bso3 = {'id': '3', 'payload': _PLD}
        bso4 = {'id': '4', 'payload': _PLD}
        bsos = [bso3, bso4]
        res = self.app.post_json(self.root + '/storage/tabs', bsos)
        ts2 = float(res.headers["X-Last-Modified"])
        self.assertTrue(ts1 < ts2)

        # asking for bsos using newer=ts where newer is the timestamps
        # of bso 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/tabs?newer=%s' % ts1)
        res = res.json
        res.sort()
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

            # we get a 503 for both POST and PUT
            self.app.post_json(self.root + '/storage/tabs', bsos,
                               status=503)
            self.app.put_json(self.root + '/storage/tabs/sure', bso1,
                              status=503)
        finally:
            for key in self.config.registry:
                if key.startswith("syncstorage:storage:"):
                    storage = self.config.registry[key]
                    if isinstance(storage.cache, BadCache):
                        storage.cache = storage.cache.cache

    def test_write_tabs_ConflictError(self):
        # This can't be run against a live server.
        if self.distant:
            raise unittest2.SkipTest

        class BadCache(object):
            """Cache client stub that raises ConflictError on write."""

            def __init__(self, cache):
                self.cache = cache

            def cas(self, key, *args, **kw):
                if key.endswith(":tabs"):
                    raise ConflictError()
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

            # on batch, we get back a 503
            self.app.post_json(self.root + '/storage/tabs', bsos,
                               status=503)

            # on single PUT, we get a 503
            self.app.put_json(self.root + '/storage/tabs/sure', bso1,
                              status=503)
        finally:
            for key in self.config.registry:
                if key.startswith("syncstorage:storage:"):
                    storage = self.config.registry[key]
                    if isinstance(storage.cache, BadCache):
                        storage.cache = storage.cache.cache


class TestStoragePaginated(TestStorage):
    """Storage testcases run using lots of internal pagination."""

    TEST_INI_FILE = "tests-paginated.ini"


class TestStorageWithBatchUploadDisabled(TestStorage):
    """Storage testcases run with batch uploads disabled via feature flag."""

    TEST_INI_FILE = "tests-no-batch.ini"

    def test_batches(self):
        # This is the same sequence of requests as the master
        # test, but without the checks for batch semantics.
        # It lets us know that batch stuff is properly ignored.

        endpoint = self.root + '/storage/col2'

        bso1 = {'id': '12', 'payload': 'elegance'}
        bso2 = {'id': '13', 'payload': 'slovenly'}
        bsos = [bso1, bso2]
        self.app.post_json(endpoint, bsos)

        bso3 = {'id': 'a', 'payload': 'internal'}
        bso4 = {'id': 'b', 'payload': 'pancreas'}
        resp = self.app.post_json(endpoint + '?batch=true', [bso3, bso4])
        assert 'batch' not in resp.json
        batch = '123456'

        bso5 = {'id': 'c', 'payload': 'tinsel'}
        bso6 = {'id': '13', 'payload': 'portnoy'}
        bso0 = {'id': '14', 'payload': 'itsybitsy'}
        commit = '?batch={0}&commit=true'.format(batch)
        # This errors out because it's trying to use an in-flight batch.
        resp = self.app.post_json(endpoint + commit, [bso5, bso6, bso0],
                                  status=400)
        # This requests a new batch, which is silently ignored and succeeds.
        commit = '?batch=true&commit=true'.format(batch)
        resp = self.app.post_json(endpoint + commit, [bso5, bso6, bso0])
        assert 'batch' not in resp.json
        committed = resp.json['modified']
        self.assertEquals(resp.json['modified'],
                          float(resp.headers['X-Last-Modified']))

        # make sure the changes applied
        resp = self.app.get(endpoint)
        res = resp.json
        res.sort()
        self.assertEquals(res, ['12', '13', '14', 'a', 'b', 'c'])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 6)
        resp = self.app.get(endpoint + '/13')
        self.assertEquals(resp.json['payload'], 'portnoy')
        self.assertEquals(committed, float(resp.headers['X-Last-Modified']))
        self.assertEquals(committed, resp.json['modified'])
        resp = self.app.get(endpoint + '/c')
        self.assertEquals(resp.json['payload'], 'tinsel')
        self.assertEquals(committed, resp.json['modified'])
        resp = self.app.get(endpoint + '/14')
        self.assertEquals(resp.json['payload'], 'itsybitsy')
        self.assertEquals(committed, resp.json['modified'])
        assert 'batch' not in resp.json

    def test_we_dont_need_no_stinkin_batches(self):
        endpoint = self.root + '/storage/col2'

        # invalid batch ID is still an error when preffed off
        bso1 = {'id': 'f', 'payload': 'pantomime'}
        self.app.post_json(endpoint + '?batch=sammich', [bso1], status=400)

        # commit with no batch ID is not an error when preffed off
        self.app.post_json(endpoint + '?commit=true', [])

    def test_batch_partial_update(self):
        # Without batch uploads, there's nothing for this test to test.
        pass

    def test_batch_ttl_update(self):
        # Without batch uploads, there's nothing for this test to test.
        pass

    def test_batch_ttl_is_based_on_commit_timestamp(self):
        # Without batch uploads, there's nothing for this test to test.
        pass

    def test_batch_size_limits(self):
        limits = self.app.get(self.root + '/info/configuration').json
        # Without batch uploads, many config limits are not present.
        self.assertTrue('max_post_records' not in limits)
        self.assertTrue('max_post_bytes' not in limits)
        self.assertTrue('max_total_records' not in limits)
        self.assertTrue('max_total_bytes' not in limits)
        self.assertTrue('max_record_payload_bytes' in limits)

    def test_batch_with_failing_bsos(self):
        # Without batch uploads, there's nothing for this test to test.
        pass

    def test_batch_id_is_correctly_scoped_to_a_user(self):
        # Without batch uploads, there's nothing for this test to test.
        pass

    def test_batch_id_is_correctly_scoped_to_a_collection(self):
        # Without batch uploads, there's nothing for this test to test.
        pass


class TestStorageMemcachedWriteThrough(TestStorageMemcached):
    """Storage testcases run against the memcached backend, if available.

    These tests are configured to use the write-through cache for all the
    test-related collections.
    """

    TEST_INI_FILE = "tests-memcached-writethrough.ini"

    def test_write_tabs_ConflictError(self):
        # ConflictErrors in the cache are ignored in write-through mode,
        # since it can just lazily re-populate from the db.
        pass


class TestStorageMemcachedCacheOnly(TestStorageMemcached):
    """Storage testcases run against the memcached backend, if available.

    These tests are configured to use the cache-only-collection behaviour
    for all the test-related collections.
    """

    TEST_INI_FILE = "tests-memcached-cacheonly.ini"


if __name__ == "__main__":
    # When run as a script, this file will execute the
    # functional tests against a live webserver.
    res = run_live_functional_tests(TestStorage, sys.argv)
    sys.exit(res)
