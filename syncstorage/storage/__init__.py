# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import abc

from mozsvc.plugin import load_from_settings
from pyramid.threadlocal import get_current_registry


class StorageError(Exception):
    """Base class for exceptions raised from the storage backend."""
    pass


class StorageConflictError(StorageError):
    """Exception raised when attempting a conflicting write."""
    pass


class SyncStorage(object):
    """Abstract Base Class for storage backends."""
    __metaclass__ = abc.ABCMeta

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
    # Collections APIs
    #

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
        """Returns True if an item exists in the database (ignoring ttls).

        Args:
            - user_id: integer identifying the user in the storage.
            - collection_name: name of the collection.
            - item_id: string identifying the item

        Returns:
            True if the item exists, False otherwise.
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
            A list of dict containing the information for the items.
            If the collection does not exist, returns None.
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
            If the item does not exist, returns None.
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
            The last-modified time of the item, or None if it did not exist.
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
            True if an item was deleted, False otherwise.
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
            True if the collection existed, False otherwise.
        """

    @abc.abstractmethod
    def get_total_size(self, user_id):
        """Returns the total size in KB of a user storage.

        The size is the sum of stored payloads.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            The size in Kbytes (float)
        """

    @abc.abstractmethod
    def get_size_left(self, user_id):
        """Returns the remaining size in KB of a user storage.

        The remaining size is calculated by substracting the
        max size and the used size.

        Args:
            user_id: integer identifying the user in the storage.

        Returns:
            The remaining size in Kbytes (float)
        """


def get_storage(request):
    """Returns a storage backend instance, given a request object.

    This function retrieves the appropriate storage backend instance to
    use for a given request.  It will use a host-specific backend if one
    is available, or fall back to the default backend if not.
    """
    # Strip the port if they happened to send it.
    host_name = request.host.rsplit(":", 1)[0]
    try:
        return request.registry["syncstorage:storage:host:" + host_name]
    except KeyError:
        return request.registry["syncstorage:storage:default"]


def includeme(config):
    """Load the storage backends for use by the given configurator.

    This function finds all storage backend declarations in the given
    configurator, creates the corresponding objects and caches them in
    the registry.  The backend to use for a specific request can then
    be looked up by calling get_storage(request).
    """
    settings = config.registry.settings
    # Find all the hostnames that have custom storage backend settings.
    hostnames = set()
    host_token = "host."
    for cfgkey in settings:
        if cfgkey.startswith(host_token):
            # Get the hostname from the config key. This assumes
            # that host-specific keys have two trailing components
            # that specify the setting to override.
            # E.g: "host:localhost.storage.sqluri" => "localhost"
            hostname = cfgkey[len(host_token):].rsplit(".", 2)[0]
            hostnames.add(hostname)
    # Create and cache the backend for each such host.
    for hostname in hostnames:
        host_cache_key = "syncstorage:storage:host:" + hostname
        host_settings = settings.getsection(host_token + hostname)
        host_settings.setdefaults(settings)
        storage = load_from_settings("storage", host_settings)
        config.registry[host_cache_key] = storage
    # Create the default backend to be used by all other hosts.
    storage = load_from_settings("storage", settings)
    config.registry["syncstorage:storage:default"] = storage
    # Scan for additional config from any storage plugins.
    # Some might fail to import, use the onerror callback to ignore them.
    config.scan("syncstorage.storage", onerror=_ignore_import_errors)


def _ignore_import_errors(name):
    """Venusian scan callback that will ignore any ImportError instances."""
    if not issubclass(sys.exc_info()[0], ImportError):
        raise
    logger = get_current_registry()['metlog']
    logger.exception("Error while scanning package %r" % (name,))
