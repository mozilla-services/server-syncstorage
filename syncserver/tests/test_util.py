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
import time
from base64 import encodestring

from syncserver.util import (authenticate_user, convert_config, bigint2time,
                              time2bigint)


class Request(object):

    def __init__(self, path_info, environ):
        self.path_info = path_info
        self.environ = environ


class AuthTool(object):

    def authenticate_user(self, *args):
        return 1


class TestUtil(unittest.TestCase):

    def test_authenticate_user(self):

        token = 'Basic ' + encodestring('tarek:tarek')
        req = Request('/1.0/tarek/info/collections', {})
        res = authenticate_user(req, AuthTool())
        self.assertEquals(res, None)

        # authenticated by auth
        req = Request('/1.0/tarek/info/collections',
                {'Authorization': token})
        res = authenticate_user(req, AuthTool())
        self.assertEquals(res, 1)

    def test_convert_config(self):
        config = {'one': '1', 'two': 'bla', 'three': 'false'}
        config = convert_config(config)

        self.assertTrue(config['one'])
        self.assertEqual(config['two'], 'bla')
        self.assertFalse(config['three'])

    def test_bigint2time(self):
        self.assertEquals(bigint2time(None), None)

    def test_time2bigint(self):
        now = time.time()
        self.assertAlmostEqual(bigint2time(time2bigint(now)), now, places=1)
