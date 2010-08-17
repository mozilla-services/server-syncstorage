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
import datetime

from sqlalchemy.sql import text

from weaveserver.tests.support import initenv
from weaveserver.auth.sql import SQLAuth
from weaveserver.auth import WeaveAuth
from weaveserver.util import ssha

WeaveAuth.register(SQLAuth)


class TestSQLAuth(unittest.TestCase):

    def setUp(self):
        self.appdir, self.config, self.storage, self.auth = initenv()
        # we don't support other storages for this test
        assert self.auth.sqluri.split(':/')[0] in ('mysql', 'sqlite')

        # lets add a user tarek/tarek
        password = ssha('tarek')
        query = text('insert into users (username, password_hash) '
                     'values (:username, :password)')
        self.auth._engine.execute(query, username='tarek', password=password)

    def test_authenticate_user(self):
        self.assertEquals(self.auth.authenticate_user('tarek', 'xxx'), None)
        self.assertEquals(self.auth.authenticate_user('tarek', 'tarek'), 1)

    def test_reset_code(self):
        self.assertFalse(self.auth.verify_reset_code(1, 'x'))

        # normal behavior
        code = self.auth.generate_reset_code(1)
        self.assertFalse(self.auth.verify_reset_code(1, 'BADCODE'))
        self.assertTrue(self.auth.verify_reset_code(1, code))

        # reseted
        code = self.auth.generate_reset_code(1)
        self.auth.clear_reset_code(1)
        self.assertFalse(self.auth.verify_reset_code(1, code))

        # expired
        code = self.auth.generate_reset_code(1)
        expiration = datetime.datetime.now() + datetime.timedelta(hours=-7)

        query = ('update users set reset_expiration = :expiration '
                 'where id = 1')
        self.auth._engine.execute(text(query), expiration=expiration)
        self.assertFalse(self.auth.verify_reset_code(1, code))


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestSQLAuth))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
