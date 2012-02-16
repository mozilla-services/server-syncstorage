# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest

from syncstorage.bso import BSO


class TestBSO(unittest.TestCase):

    def test_basic(self):
        bso = BSO()
        bso = BSO({'boooo': ''})
        self.assertTrue('boooo' not in bso)

    def test_validation(self):
        data = {'id': 'bigid' * 30}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)

        data = {'sortindex': 9999999999}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)

        data = {'sortindex': '9999'}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertTrue(result)
        self.assertTrue(bso['sortindex'], 9999)

        data = {'sortindex': 'ok'}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)

        data = {'sortindex': '12'}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertTrue(result)
        self.assertEquals(bso['sortindex'], 12)

        for bad_ttl in ('bouh', -1, 31537000):
            data = {'ttl': bad_ttl}
            bso = BSO(data)
            result, failure = bso.validate()
            self.assertFalse(result)

        data = {'ttl': 3600}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertTrue(result)

        data = {'payload':  "X" * 30000}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertTrue(result)

        data = {'payload':  "X" * 300000}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBSO))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
