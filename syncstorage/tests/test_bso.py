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
        data = {'parentid': 'bigid' * 30}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)

        data = {'parentid': 'id', 'sortindex': 9999999999}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)

        data = {'parentid': 'id', 'sortindex': '9999.1'}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertTrue(result)
        self.assertTrue(bso['sortindex'], 9999)

        data = {'parentid': 'id', 'sortindex': 'ok'}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertFalse(result)

        data = {'parentid':  33, 'sortindex': '12'}
        bso = BSO(data)
        result, failure = bso.validate()
        self.assertTrue(result)
        self.assertEquals(bso['parentid'], '33')
        self.assertEquals(bso['sortindex'], 12)

        for bad_ttl in ('bouh', -1, 31537000):
            data = {'parentid':  33, 'ttl': bad_ttl}
            bso = BSO(data)
            result, failure = bso.validate()
            self.assertFalse(result)

        data = {'parentid':  33, 'ttl': 3600}
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
