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

from weave.server.storage import get_storage

_UID = 1


class TestSQLStorage(unittest.TestCase):

    def setUp(self):
        self.storage = get_storage('sql', sqluri='sqlite:///:memory:')

    def tearDown(self):
        self.storage.delete_user(_UID)

    def test_user_exists(self):
        self.assertFalse(self.storage.user_exists(_UID))

    def test_set_get_user(self):
        self.assertFalse(self.storage.user_exists(_UID))
        self.storage.set_user(_UID, username='tarek', email='tarek@ziade.org')
        self.assertTrue(self.storage.user_exists(_UID))
        self.storage.set_user(_UID, email='tarek2@ziade.org')
        res = self.storage.get_user(_UID, fields=['email'])
        self.assertEquals(res, (u'tarek2@ziade.org',))

    def test_collections(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.assertFalse(self.storage.collection_exists(_UID, 'My collection'))
        self.storage.set_collection(_UID, 'My collection')
        self.assertTrue(self.storage.collection_exists(_UID, 'My collection'))

        res = self.storage.get_collection(_UID, 'My collection')
        self.assertEquals(res, (1, 1, u'My collection'))
        res = self.storage.get_collection(_UID, 'My collection',
                                          fields=['name'])
        self.assertEquals(res, (u'My collection',))

        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 1)
        self.assertEquals(res[0], (1, 1, u'My collection'))
        res = self.storage.get_collections(_UID, fields=['name'])
        self.assertEquals(res[0], (u'My collection',))

        # adding a new collection
        self.storage.set_collection(_UID, 'My collection 2')
        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 2)

        names = self.storage.get_collection_names(_UID)
        self.assertEquals(names, [(u'My collection',), (u'My collection 2',)])

        # removing a collection
        self.storage.delete_collection(_UID, 'My collection 2')
        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 1)

        # removing *all*
        self.storage.delete_user(_UID)
        res = self.storage.get_collections(_UID)
        self.assertEquals(len(res), 0)
        self.assertFalse(self.storage.user_exists(_UID))

    def test_items(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col')
        self.assertFalse(self.storage.item_exists(_UID, 'col', 1))
        self.assertEquals(self.storage.get_items(_UID, 'col'), [])

        self.storage.set_item(_UID, 'col', 1, payload='XXX')
        res = self.storage.get_item(_UID, 'col', 1)
        self.assertEquals(res.payload, 'XXX')

        self.storage.set_item(_UID, 'col', 2, payload='XXX')

        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 2)

        self.storage.delete_item(_UID, 'col', 1)
        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 1)

        self.storage.delete_items(_UID, 'col')
        items = self.storage.get_items(_UID, 'col')
        self.assertEquals(len(items), 0)

    def test_get_collection_timestamps(self):
        self.storage.set_user(_UID, email='tarek@ziade.org')
        self.storage.set_collection(_UID, 'col1')
        self.storage.set_collection(_UID, 'col2')

        self.storage.set_item(_UID, 'col1', 1, payload='XXX')
        self.storage.set_item(_UID, 'col2', 1, payload='XXX')

        timestamps = self.storage.get_collection_timestamps(_UID)
        names = [entry[0] for entry in timestamps]
        names.sort()
        self.assertEquals(names, ['col1', 'col2'])


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestSQLStorage))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
