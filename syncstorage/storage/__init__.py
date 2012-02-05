# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

 users
   |
   -- collections
           |
           |
           ---- items

"""
import abc
from services.pluginreg import PluginRegistry


class SyncStorage(PluginRegistry):
    """Abstract Base Class for the storage."""
    plugin_type = 'storage'

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

    @abc.abstractmethod
    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            A dict containing the name and size for each collection.
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
    def set_item(self, user_id, collection_name, item_id, storage_time,
                 **values):
        """Sets an item.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item
            - storage_time: time of the storage, if none provided, use current
              time

            - values: mapping containing the values.

        Returns:
            A dict containing the information for the item
        """

    @abc.abstractmethod
    def set_items(self, user_id, collection_name, items, storage_time=None):
        """Adds or update a batch of items.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - items: a list of dict
            - storage_time: time of the storage, if none provided, use current
              time
        Returns:
            Integer: number of inserts/updates
        """

    @abc.abstractmethod
    def delete_item(self, user_id, collection_name, item_id,
                    storage_time=None):
        """Deletes an item

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item
            - values: mapping containing the values.
            - storage_time: time of the storage, if none provided, use current
              time
        Returns:
            None
        """

    @abc.abstractmethod
    def delete_items(self, user_id, collection_name, item_ids=None,
                     storage_time=None):
        """Deletes items. All items are removed unless item_ids is provided.

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_ids: if provided, its the ids of the items to be removed.
              all items will be removed if not provided.
            - storage_time: time of the storage, if none provided, use current
              time
        Returns:
            None
        """

    @abc.abstractmethod
    def get_total_size(self, user_id, recalculate):
        """Returns the total size in KB of a user storage.

        The size is the sum of stored payloads.

        Args:
            user_id: integer identifying the user in the storage.
            recalculate: if True, kill any cache

        Returns:
            The size in Kbytes (float)
        """

    @abc.abstractmethod
    def get_size_left(self, user_id, recalculate=False):
        """Returns the remaining size in KB of a user storage.

        The remaining size is calculated by substracting the
        max size and the used size.

        Args:
            user_id: integer identifying the user in the storage.
            recalculate: if True, kill any cache

        Returns:
            The remaining size in Kbytes (float)
        """


def get_storage(config):
    """Returns a storage backend instance, given a config.

    "config" is a mapping, containing the configuration.
    All keys that starts with "auth." are used in the function.

    - "storage.backend" must be present and contain a fully qualified name of a
      backend class to be used, or the name of any backend synccore provides.
      "sql" or "memcachedsql".

    - other keys that starts with "storage." are passed to the backend
      constructor -- with the prefix stripped.
    """
    return SyncStorage.get_from_config(config, 'storage')
