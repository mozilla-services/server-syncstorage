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
