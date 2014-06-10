# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Functional tests from version 1.1 of the sync storage protocol.

This file is a copy of the functional tests from sync1.1, minimally
edited to run against the sync1.5 server.  They're here to help verify
that sync1.1 clients can still sync successfully agains the sync1.5 server
if they stick to the parts of the spec that were in use in practice.

"""

import sys
import time
import random
import string
import simplejson as json
from decimal import Decimal

from syncstorage.tests.functional.support import StorageFunctionalTestCase
from syncstorage.tests.functional.support import run_live_functional_tests

WEAVE_INVALID_WBO = 8


_PLD = '*' * 500
_ASCII = string.ascii_letters + string.digits


def randtext(size=10):
    return ''.join([random.choice(_ASCII) for i in range(size)])


class TestOldStorage(StorageFunctionalTestCase):

    def setUp(self):
        super(TestOldStorage, self).setUp()
        self.root = '/1.5/%d' % self.user_id
        self.app.delete(self.root + '/storage',
                        headers={'X-Confirm-Delete': '1'})
        # let's create some collections for our tests
        for name in ('client', 'crypto', 'forms', 'history', 'col1', 'col2'):
            self.app.post_json(self.root + '/storage/' + name, [])

        for item in range(3):
            self.app.put_json(self.root + '/storage/col1/' + str(item),
                              {'payload': 'xxx'})
            time.sleep(0.02)   # make sure we have different timestamps

        for item in range(5):
            self.app.put_json(self.root + '/storage/col2/' + str(item),
                              {'payload': 'xxx'})
            time.sleep(0.02)   # make sure we have different timestamps

    def test_get_collections(self):
        resp = self.app.get(self.root + '/info/collections')
        res = resp.json
        keys = res.keys()
        self.assertTrue(len(keys), 2)
        self.assertEquals(int(resp.headers['X-Weave-Records']), len(keys))

    def test_get_collection_count(self):
        resp = self.app.get(self.root + '/info/collection_counts')
        res = resp.json
        values = res.values()
        values.sort()
        self.assertEquals(values, [3, 5])
        self.assertEquals(int(resp.headers['X-Weave-Records']), 2)

    def test_get_collection(self):
        resp = self.app.get(self.root + '/storage/col3')
        res = resp.json
        self.assertEquals(res, [])

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

        # "newer"
        # Returns only ids for objects in the collection that have been
        # last modified since the date given.
        self.app.delete(self.root + '/storage/col2')
        wbo = {'id': '128', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/128', wbo)
        ts = res.json

        fts = json.dumps(ts)
        time.sleep(.3)

        wbo = {'id': '129', 'payload': 'x'}
        res = self.app.put_json(self.root + '/storage/col2/129', wbo)
        ts2 = res.json

        fts2 = json.dumps(ts2)

        self.assertTrue(fts < fts2)

        res = self.app.get(self.root + '/storage/col2?newer=%s' % ts)
        res = res.json
        self.assertEquals(res, ['129'])

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

        # "limit"
        # Sets the maximum number of ids that will be returned
        self.app.delete(self.root + '/storage/col2')

        wbos = []
        for i in range(10):
            wbo = {'id': str(i), 'payload': 'x'}
            wbos.append(wbo)
        self.app.post_json(self.root + '/storage/col2', wbos)

        res = self.app.get(self.root + '/storage/col2?limit=2')
        res = res.json
        self.assertEquals(len(res), 2)

        res = self.app.get(self.root + '/storage/col2')
        res = res.json
        self.assertTrue(len(res) > 9)

        # "sort"
        #   'newest' - Orders by modification date (newest first)
        #   'index' - Orders by the sortindex descending (highest weight first)
        self.app.delete(self.root + '/storage/col2')

        for index, sortindex in (('0', 1), ('1', 34), ('2', 12)):
            wbo = {'id': index, 'payload': 'x', 'sortindex': sortindex}
            # XXX TODO: old server used to accept a single bso in the body
            # and transparently promote it to a list.
            self.app.post_json(self.root + '/storage/col2', [wbo])
            time.sleep(0.1)

        res = self.app.get(self.root + '/storage/col2?sort=newest')
        res = res.json
        self.assertEquals(res, ['2', '1', '0'])

        res = self.app.get(self.root + '/storage/col2?sort=index')
        res = res.json
        self.assertEquals(res, ['1', '2', '0'])

    def test_alternative_formats(self):
        # application/json
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.content_type.split(";")[0], 'application/json')

        res = res.json
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

        # application/newlines
        res = self.app.get(self.root + '/storage/col2',
                           headers=[('Accept', 'application/newlines')])
        self.assertEquals(res.content_type.split(";")[0],
                          'application/newlines')

        res = [json.loads(line) for line in res.body.strip().split('\n')]
        res.sort()
        self.assertEquals(res, ['0', '1', '2', '3', '4'])

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
        self.app.put_json(self.root + '/storage/col2/12345', wbo)
        res = self.app.get(self.root + '/storage/col2/12345')
        res = res.json
        self.assertEquals(res['payload'], _PLD)

        # now let's update it
        wbo = {'payload': 'YYY'}
        self.app.put_json(self.root + '/storage/col2/12345', wbo)
        res = self.app.get(self.root + '/storage/col2/12345')
        res = res.json
        self.assertEquals(res['payload'], 'YYY')

    def test_set_collection(self):
        # sending two wbos
        wbo1 = {'id': '12', 'payload': _PLD}
        wbo2 = {'id': '13', 'payload': _PLD}
        wbos = [wbo1, wbo2]
        self.app.post_json(self.root + '/storage/col2', wbos)

        # checking what we did
        res = self.app.get(self.root + '/storage/col2/12')
        res = res.json
        self.assertEquals(res['payload'], _PLD)
        res = self.app.get(self.root + '/storage/col2/13')
        res = res.json
        self.assertEquals(res['payload'], _PLD)

        # one more time, with changes
        wbo1 = {'id': '13', 'payload': 'XyX'}
        wbo2 = {'id': '14', 'payload': _PLD}
        wbos = [wbo1, wbo2]
        self.app.post_json(self.root + '/storage/col2', wbos)

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
        wbos = [wbo1, wbo2]
        self.app.post_json(self.root + '/storage/col2', wbos)
        self.app.get(self.root + '/storage/col2/two', status=404)

    def test_collection_usage(self):
        self.app.delete(self.root + '/storage',
                        headers=[('X-Confirm-Delete', '1')])

        wbo1 = {'id': '13', 'payload': 'XyX'}
        wbo2 = {'id': '14', 'payload': _PLD}
        wbos = [wbo1, wbo2]
        self.app.post_json(self.root + '/storage/col2', wbos)

        res = self.app.get(self.root + '/info/collection_usage')
        usage = res.json
        col2_size = usage['col2']
        wanted = len(wbo1['payload']) + len(wbo2['payload'])
        self.assertEqual(col2_size, wanted / 1024.)

    def test_delete_collection(self):
        self.app.delete(self.root + '/storage/col2')

        # creating a collection of three
        wbo1 = {'id': '12', 'payload': _PLD}
        wbo2 = {'id': '13', 'payload': _PLD}
        wbo3 = {'id': '14', 'payload': _PLD}
        wbos = [wbo1, wbo2, wbo3]
        self.app.post_json(self.root + '/storage/col2', wbos)
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
        self.app.post_json(self.root + '/storage/col2', wbos)
        self.app.delete(self.root + '/storage/col2?ids=12,14')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)
        self.app.delete(self.root + '/storage/col2?ids=13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 0)

    def test_delete_item(self):
        self.app.delete(self.root + '/storage/col2')

        # creating a collection of three
        wbo1 = {'id': '12', 'payload': _PLD}
        wbo2 = {'id': '13', 'payload': _PLD}
        wbo3 = {'id': '14', 'payload': _PLD}
        wbos = [wbo1, wbo2, wbo3]
        self.app.post_json(self.root + '/storage/col2', wbos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # deleting item 13
        self.app.delete(self.root + '/storage/col2/13')
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 2)

        # unexisting item should return a 404
        # XXX TODO: in sync1.1 it would return a 200
        self.app.delete(self.root + '/storage/col2/12982', status=404)

    def test_delete_storage(self):
        self.app.delete(self.root + '/storage/col2')

        # creating a collection of three
        wbo1 = {'id': '12', 'payload': _PLD}
        wbo2 = {'id': '13', 'payload': _PLD}
        wbo3 = {'id': '14', 'payload': _PLD}
        wbos = [wbo1, wbo2, wbo3]
        self.app.post_json(self.root + '/storage/col2', wbos)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 3)

        # also populate some items that get special caching treatment.
        wbo = {'payload': _PLD}
        self.app.put_json(self.root + '/storage/meta/global', wbo)
        self.app.put_json(self.root + '/storage/tabs/home', wbo)
        res = self.app.get(self.root + '/storage/meta/global')
        self.assertEquals(res.json["payload"], _PLD)
        res = self.app.get(self.root + '/storage/tabs/home')
        self.assertEquals(res.json["payload"], _PLD)

        # deleting all with no confirmation
        # self.app.delete(self.root + '/storage', status=400)

        # deleting all for real now
        res = self.app.delete(self.root + '/storage',
                              headers=[("X-Confirm-Delete", "1")])
        res = json.loads(res.body)
        items = self.app.get(self.root + '/storage/col2').json
        self.assertEquals(items, [])
        self.app.get(self.root + '/storage/meta/global', status=404)
        self.app.get(self.root + '/storage/tabs/home', status=404)

    def test_x_weave_timestamp(self):
        if self.distant:
            return

        now = time.time()
        res = self.app.get(self.root + '/storage/col2')
        diff = abs(now - float(res.headers['X-Weave-Timestamp']))
        self.assertTrue(diff < 0.1)

        # getting the timestamp with a PUT
        wbo = {'payload': _PLD}
        now = time.time()
        res = self.app.put_json(self.root + '/storage/col2/12345', wbo)
        diff = abs(now - float(res.headers['X-Weave-Timestamp']))
        self.assertTrue(diff < 0.2)

        # getting the timestamp with a POST
        wbo1 = {'id': 12, 'payload': _PLD}
        wbo2 = {'id': 13, 'payload': _PLD}
        wbos = [wbo1, wbo2]
        now = time.time()
        res = self.app.post_json(self.root + '/storage/col2', wbos)
        self.assertTrue(abs(now -
                        float(res.headers['X-Weave-Timestamp'])) < 0.2)

    def test_ifunmodifiedsince(self):
        wbo = {'payload': _PLD}
        ts = self.app.put_json(self.root + '/storage/col2/12345', wbo)
        ts = json.loads(ts.body) - 1000
        self.app.put_json(self.root + '/storage/col2/12345', wbo,
                          headers=[('X-If-Unmodified-Since', str(ts))],
                          status=412)

    def test_quota(self):
        res = self.app.get(self.root + '/info/quota')
        old_used, quota = res.json
        wbo = {'payload': _PLD}
        self.app.put_json(self.root + '/storage/col2/12345', wbo)
        res = self.app.get(self.root + '/info/quota')
        used, quota = res.json
        self.assertEquals(used - old_used, len(_PLD) / 1024.)

    def test_get_collection_ttl(self):
        self.app.delete(self.root + '/storage/col2')
        wbo = {'payload': _PLD, 'ttl': 0}
        res = self.app.put_json(self.root + '/storage/col2/12345', wbo)
        time.sleep(1.1)
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(res.json, [])

        wbo = {'payload': _PLD, 'ttl': 2}
        res = self.app.put_json(self.root + '/storage/col2/123456', wbo)

        # it should exists now
        res = self.app.get(self.root + '/storage/col2')
        self.assertEquals(len(res.json), 1)

        # trying a second put again
        self.app.put_json(self.root + '/storage/col2/123456', wbo)

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
        res = self.app.post_json(self.root + '/storage/col2', wbos)
        res = res.json
        self.assertEquals(len(res['success']), 70)
        self.assertEquals(len(res['failed']), 0)
        # The test config has max_count=100.
        # Uploading 105 items should produce five failures.
        wbos = [{'id': str(i), 'payload': _PLD} for i in range(105)]
        res = self.app.post_json(self.root + '/storage/col2', wbos)
        res = res.json
        self.assertEquals(len(res['success']), 100)
        self.assertEquals(len(res['failed']), 5)
        # The test config has max_bytes=1M.
        # Uploading 5 210MB items should produce one failure.
        wbos = [{'id': str(i), 'payload': "X" * (210 * 1024)}
                for i in range(5)]
        res = self.app.post_json(self.root + '/storage/col2', wbos)
        res = res.json
        self.assertEquals(len(res['success']), 4)
        self.assertEquals(len(res['failed']), 1)

    def test_weird_args(self):
        # pushing some data in col2
        wbos = [{'id': str(i), 'payload': _PLD} for i in range(10)]
        res = self.app.post_json(self.root + '/storage/col2', wbos)
        res = res.json

        # trying weird args and make sure the server returns 400s
        args = ('newer', 'limit', 'offset')
        for arg in args:
            self.app.get(self.root + '/storage/col2?%s=%s' % (arg, randtext()),
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
        wbos = [{'id': '{6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d}' % i,
                 'payload': _PLD} for i in range(5)]
        res = self.app.post_json(self.root + '/storage/passwords', wbos)
        res = res.json

        # now deleting some of them
        ids = ','.join(['{6820f3ca-6e8a-4ff4-8af7-8b3625d7d65%d}' % i
                        for i in range(2)])

        self.app.delete(self.root + '/storage/passwords?ids=%s' % ids)

        res = self.app.get(self.root + '/storage/passwords?ids=%s' % ids)
        self.assertEqual(res.json, [])

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
        res.sort()
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
        wbo1 = {'id': '1', 'payload': _PLD}
        wbo2 = {'id': '2', 'payload': _PLD}
        wbos = [wbo1, wbo2]
        res = self.app.post_json(self.root + '/storage/meh', wbos)
        ts = json.loads(res.body, use_decimal=True)['modified']

        # wait a bit
        time.sleep(0.2)

        # send two more wbos
        wbo3 = {'id': '3', 'payload': _PLD}
        wbo4 = {'id': '4', 'payload': _PLD}
        wbos = [wbo3, wbo4]
        res = self.app.post_json(self.root + '/storage/meh', wbos)

        # asking for wbos using newer=ts where newer is the timestamps
        # of wbo 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/meh?newer=%s' % ts)
        res = res.json
        res.sort()
        self.assertEquals(res, ['3', '4'])

    def test_strict_newer_tabs(self):
        # send two wbos in the 'tabs' collection
        wbo1 = {'id': '1', 'payload': _PLD}
        wbo2 = {'id': '2', 'payload': _PLD}
        wbos = [wbo1, wbo2]
        res = self.app.post_json(self.root + '/storage/tabs', wbos)
        ts = json.loads(res.body, use_decimal=True)['modified']

        # wait a bit
        time.sleep(0.2)

        # send two more wbos
        wbo3 = {'id': '3', 'payload': _PLD}
        wbo4 = {'id': '4', 'payload': _PLD}
        wbos = [wbo3, wbo4]
        self.app.post_json(self.root + '/storage/tabs', wbos)

        # asking for wbos using newer=ts where newer is the timestamps
        # of wbo 1 and 2, should not return them
        res = self.app.get(self.root + '/storage/tabs?newer=%s' % ts)
        res = res.json
        res.sort()
        self.assertEquals(res, ['3', '4'])

    def test_handling_of_invalid_json(self):
        # Single upload with JSON that's not a WBO.
        # It should fail with WEAVE_INVALID_WBO
        wbo = "notawbo"
        res = self.app.put_json(self.root + '/storage/col2/invalid', wbo,
                                status=400)
        self.assertEquals(int(res.body), WEAVE_INVALID_WBO)
        wbo = 42
        res = self.app.put_json(self.root + '/storage/col2/invalid', wbo,
                                status=400)
        self.assertEquals(int(res.body), WEAVE_INVALID_WBO)
        wbo = {'id': ["1", "2"], 'payload': {'3': '4'}}
        res = self.app.put_json(self.root + '/storage/col2/invalid', wbo,
                                status=400)
        self.assertEquals(int(res.body), WEAVE_INVALID_WBO)
        # Batch upload with JSON that's not a list of WBOs
        # It should fail with WEAVE_INVALID_WBO
        wbos = "notalist"
        res = self.app.post_json(self.root + '/storage/col2', wbos, status=400)
        self.assertEquals(int(res.body), WEAVE_INVALID_WBO)
        wbos = 42
        res = self.app.post_json(self.root + '/storage/col2', wbos, status=400)
        self.assertEquals(int(res.body), WEAVE_INVALID_WBO)
        # Batch upload a list with something that's not a WBO
        # It should process the good entry and fail for the bad.
        # XXX TODO: in sync1.5 we just fail the whole request
        # wbos = [{'id': '1', 'payload': 'GOOD'}, "BAD"]
        # res = self.app.post_json(self.root + '/storage/col2', wbos)
        # res = res.json
        # self.assertEquals(len(res['success']), 1)
        # self.assertEquals(len(res['failed']), 1)

    def test_that_put_reports_consistent_timestamps(self):
        # This checks for the behaviour reported in Bug 739519, where
        # the timestamp in the body of a PUT response could be different
        # from the one reported in X-Weave-Timestamp.
        wbo = {'id': 'TEST', 'payload': 'DATA'}
        res = self.app.put_json(self.root + '/storage/col2/TEST', wbo)
        for i in xrange(200):
            wbo = self.app.get(self.root + '/storage/col2/TEST').json
            res = self.app.put_json(self.root + '/storage/col2/TEST', wbo)
            self.assertEquals(float(res.body),
                              float(res.headers["X-Weave-Timestamp"]))

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


if __name__ == "__main__":
    # When run as a script, this file will execute the
    # functional tests against a live webserver.
    res = run_live_functional_tests(TestOldStorage, sys.argv)
    sys.exit(res)
