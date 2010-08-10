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
"""

 users
   |
   -- collections
           |
           |
           ---- items

"""
import abc
from weaveserver.plugin import Plugin

class WeaveStorage(Plugin):
    """Abstract Base Class for the storage."""
    name = 'storage'

    @abc.abstractmethod
    def get_name(self):
        """Returns the name of the plugin.

        Must be a class method.

        Args:
            None

        Returns:
            The plugin name
        """


    #
    # Users APIs -- the user id is the email
    #

    @abc.abstractmethod
    def user_exists(self, user_id):
        """Returns True if the user exists.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            True or False
        """

    @abc.abstractmethod
    def set_user(self, user_id, **values):
        """Sets information for a user.

        If the user doesn't exists, it will be created.

        Args:
            - user_id: integer identifying the user in the storage.
            - values: mapping containing the values.

        Returns:
            None
        """

    @abc.abstractmethod
    def get_user(self, user_id, fields=None):
        """Returns user information.

        Args:
            - user_id: integer identifying the user in the storage.
            - fields: if provided, its a list of fields to return,
              all fields are returns by default.

        Returns:
            A dict containing the values for the user.
        """

    @abc.abstractmethod
    def delete_user(self, user_id):
        """Remove a user and his data from the storage.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            None
        """


    #
    # Collections APIs
    #

    @abc.abstractmethod
    def delete_collection(self, user_id, collection_name):
        """Deletes a collection.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection

        Returns:
            None
        """

    @abc.abstractmethod
    def collection_exists(self, user_id, collection_name):
        """Returns True if the collection exists.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection

        Returns:
            True or False
        """

    @abc.abstractmethod
    def set_collection(self, user_id, collection_name, **values):
        """Creates a new collection.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - values: mapping containing the values.

        Returns:
            integer identifying the collection in the storage.
        """

    @abc.abstractmethod
    def get_collection(self, user_id, collection_name, fields=None):
        """Return information about a collection.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - fields: if provided, its a list of fields to return,
              all fields are returns by default.

        Returns:
            A dict containing the information for the collection
        """

    @abc.abstractmethod
    def get_collections(self, user_id, fields=None):
        """Returns the collections information.

        Args:
            - user_id: integer identifying the user in the storage.
            - fields: if provided, its a list of fields to return,
              all fields are returns by default.

        Returns:
            A list of dict containing the information for the collection
        """

    @abc.abstractmethod
    def get_collection_names(self, user_id):
        """Returns the collection names for a user.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            A list of dict containing the name and id for each collection.
        """

    @abc.abstractmethod
    def get_collection_timestamps(self, user_id):
        """Returns the collection timestamps for a user.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            A list of dict containing the name and timestamp for each
            collection.
        """

    @abc.abstractmethod
    def get_collection_counts(self, user_id):
        """Returns the collection counts.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            A list of dict containing the name and count for each
            collection.
        """

    #
    # Items APIs
    #

    @abc.abstractmethod
    def item_exists(self, user_id, collection_name, item_id):
        """Returns True if an item exists.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item

        Returns:
            True or False
        """

    @abc.abstractmethod
    def get_items(self, user_id, collection_name, fields=None):
        """returns items from a collection

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item
            - fields: if provided, its a list of fields to return,
              all fields are returns by default.

        Returns:
            A list of dict containing the information for the items
        """

    @abc.abstractmethod
    def get_item(self, user_id, collection_name, item_id, fields=None):
        """Returns one item.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item
            - fields: if provided, its a list of fields to return,
              all fields are returns by default.

        Returns:
            A dict containing the information for the item
        """

    @abc.abstractmethod
    def set_item(self, user_id, collection_name, item_id, **values):
        """Sets an item.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item
            - values: mapping containing the values.

        Returns:
            A dict containing the information for the item
        """

    @abc.abstractmethod
    def set_items(self, user_id, collection_name, items):
        """Adds or update a batch of items.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - items: a list of dict

        Returns:
            Integer: number of inserts/updates
        """

    @abc.abstractmethod
    def delete_item(self, user_id, collection_name, item_id):
        """Deletes an item

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item
            - values: mapping containing the values.

        Returns:
            None
        """

    @abc.abstractmethod
    def delete_items(self, user_id, collection_name, item_ids=None):
        """Deletes items. All items are removed unless item_ids is provided.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_ids: if provided, its the ids of the items to be removed.
              all items will be removed if not provided.

        Returns:
            None
        """
