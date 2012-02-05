# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import unittest
from syncstorage.storage import SyncStorage


class IAmAValidStorage(object):

    @classmethod
    def get_name(self):
        """Returns the name of the storage"""
        return 'valid'

    def user_exists(self, user_name):
        ''

    def set_user(self, user_email, **values):
        ''

    def get_user(self, user_name, fields=None):
        ''

    def delete_user(self, user_name):
        ''

    def delete_collection(self, user_name, collection_name):
        ''

    def collection_exists(self, user_name, collection_name):
        ''

    def set_collection(self, user_name, collection_name, **values):
        ''

    def get_collection(self, user_name, collection_name, fields=None):
        ''

    def get_collections(self, user_name, fields=None):
        ''

    def get_collection_names(self, user_name):
        ''

    def get_collection_timestamps(self, user_name):
        ''

    def get_collection_counts(self, user_name):
        ''

    def item_exists(self, user_name, collection_name, item_id):
        ''

    def get_items(self, user_name, collection_name, fields=None):
        ''

    def get_item(self, user_name, collection_name, item_id, fields=None):
        ''

    def set_item(self, user_name, collection_name, item_id, **values):
        ''

    def set_items(self, user_name, collection_name, item_id, items):
        ''

    def delete_item(self, user_name, collection_name, item_id):
        ''

    def delete_items(self, user_name, collection_name, item_ids=None):
        ''

    def get_total_size(self, user_id):
        ''

    def get_collection_sizes(self, user_id):
        ''

    def get_size_left(user_id):
        ''


class TestSyncStorageBase(unittest.TestCase):

    def test_register(self):

        class NotAStorage(object):
            pass

        self.assertRaises(TypeError, SyncStorage.register, NotAStorage)
        SyncStorage.register(IAmAValidStorage)
        fqn = 'syncstorage.tests.test_storagebase.IAmAValidStorage'
        self.assert_(isinstance(SyncStorage.get(fqn), IAmAValidStorage))


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestSyncStorageBase))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
