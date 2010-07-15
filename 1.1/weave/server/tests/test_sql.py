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
# The Initial Developer of the Original Code is Mozilla Labs.
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

from sqlalchemy import create_engine

from weave.server.storage import get_storage, _BACKENDS
from weave.server.storage import sql

_TEST_USER = 'blabla@foo.com'


class TestSQLStorage(unittest.TestCase):

    def setUp(self):
        self.old_SQLURI = sql._SQLURI
        sql._SQLURI = 'sqlite:///:memory:'
        self.old_engine = sql.engine
        sql.engine = create_engine(sql._SQLURI)
        for table in sql.tables:
            table.metadata.bind = sql.engine
            table.create()

        _BACKENDS['sql'] = sql.WeaveSQLStorage()

    def tearDown(self):
        sql._SQLURI = self.old_SQLURI
        sql.engine = self.old_engine
        for table in sql.tables:
            table.metadata.bind = sql.engine

    def test_user_exists(self):
        storage = get_storage('sql')
        self.assertFalse(storage.user_exists(_TEST_USER))

    def test_set_get_user(self):
        storage = get_storage('sql')
        self.assertFalse(storage.user_exists(_TEST_USER))
        storage.set_user(_TEST_USER, email='tarek@ziade.org')
        self.assertTrue(storage.user_exists(_TEST_USER))
        storage.set_user(_TEST_USER, email='tarek2@ziade.org')
        res = storage.get_user(_TEST_USER, fields=['email'])
        self.assertEquals(res, (u'tarek2@ziade.org',))


    def test_collections(self):
        storage = get_storage('sql')
        storage.set_user(_TEST_USER, email='tarek@ziade.org')

        self.assertFalse(storage.collection_exists(_TEST_USER, 'My collection'))
        storage.set_collection(_TEST_USER, 'My collection')
        self.assertTrue(storage.collection_exists(_TEST_USER, 'My collection'))

        res = storage.get_collection(_TEST_USER, 'My collection')
        self.assertEquals(res, (1, 1, u'My collection'))
        res = storage.get_collection(_TEST_USER, 'My collection',
                                     fields=['name'])
        self.assertEquals(res, (u'My collection',))

        res = storage.get_collections(_TEST_USER)
        self.assertEquals(len(res), 1)
        self.assertEquals(res[0], (1, 1, u'My collection'))
        res = storage.get_collections(_TEST_USER,
                                      fields=['name'])
        self.assertEquals(res[0], (u'My collection',))

        # adding a new collection
        storage.set_collection(_TEST_USER, 'My collection 2')
        res = storage.get_collections(_TEST_USER)
        self.assertEquals(len(res), 2)

        names = storage.get_collection_names(_TEST_USER)
        self.assertEquals(names, [(u'My collection',), (u'My collection 2',)])

        # removing a collection
        storage.delete_collection(_TEST_USER, 'My collection 2')
        res = storage.get_collections(_TEST_USER)
        self.assertEquals(len(res), 1)

        # removing *all*
        storage.delete_user(_TEST_USER)
        res = storage.get_collections(_TEST_USER)
        self.assertEquals(len(res), 0)
        self.assertFalse(storage.user_exists(_TEST_USER))

    def test_items(self):
        storage = get_storage('sql')
        storage.set_user(_TEST_USER, email='tarek@ziade.org')
        storage.set_collection(_TEST_USER, 'col')
        self.assertFalse(storage.item_exists(_TEST_USER, 'col', 1))
        self.assertEquals(storage.get_items(_TEST_USER, 'col'), [])

        storage.set_item(_TEST_USER, 'col', 1, payload='XXX')
        res = storage.get_item(_TEST_USER, 'col', 1)
        self.assertEquals(res.payload, 'XXX')

        storage.set_item(_TEST_USER, 'col', 2, payload='XXX')

        items = storage.get_items(_TEST_USER, 'col')
        self.assertEquals(len(items), 2)

        storage.delete_item(_TEST_USER, 'col', 1)
        items = storage.get_items(_TEST_USER, 'col')
        self.assertEquals(len(items), 1)

        storage.delete_items(_TEST_USER, 'col')
        items = storage.get_items(_TEST_USER, 'col')
        self.assertEquals(len(items), 0)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
