# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest

from syncstorage.wbo import WBO


class TestWBO(unittest.TestCase):

    def test_basic(self):
        wbo = WBO()
        wbo = WBO({'boooo': ''})
        self.assertTrue('boooo' not in wbo)

    def test_validation(self):
        data = {'parentid': 'bigid' * 30}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertFalse(result)

        data = {'parentid': 'id', 'sortindex': 9999999999}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertFalse(result)

        data = {'parentid': 'id', 'sortindex': '9999.1'}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertTrue(result)
        self.assertTrue(wbo['sortindex'], 9999)

        data = {'parentid': 'id', 'sortindex': 'ok'}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertFalse(result)

        data = {'parentid':  33, 'sortindex': '12'}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertTrue(result)
        self.assertEquals(wbo['parentid'], '33')
        self.assertEquals(wbo['sortindex'], 12)

        for bad_ttl in ('bouh', -1, 31537000):
            data = {'parentid':  33, 'ttl': bad_ttl}
            wbo = WBO(data)
            result, failure = wbo.validate()
            self.assertFalse(result)

        data = {'parentid':  33, 'ttl': 3600}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertTrue(result)

        data = {'payload':  "X" * 30000}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertTrue(result)

        data = {'payload':  "X" * 300000}
        wbo = WBO(data)
        result, failure = wbo.validate()
        self.assertFalse(result)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestWBO))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
