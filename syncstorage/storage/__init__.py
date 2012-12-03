# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

Abstract interface definition for storage backends.

"""

import sys
import abc

from mozsvc.plugin import resolve_name
from pyramid.threadlocal import get_current_registry


class StorageError(Exception):
    """Base class for exceptions raised from the storage backend."""
    pass


class ConflictError(StorageError):
    """Exception raised when attempting a conflicting write."""
    pass


class NotFoundError(StorageError):
    """Exception raised when accessing something that does not exist."""
    pass


class CollectionNotFoundError(NotFoundError):
    """Exception raised when accessing a collection that does not exist."""
    pass


class ItemNotFoundError(NotFoundError):
    """Exception raised when accessing an item that does not exist."""
    pass


class InvalidOffsetError(StorageError, ValueError):
    """Exception raised when an invalid offset token is provided."""
    pass


class SyncStorage(object):
    """Abstract Base Class for storage backends.

    Backend implementations for the SyncStorage server must implement the
    interface defined by this class.

    To allow a consistent view of the stored data in the face of concurrent
    access, backends are required to support a simple locking API.  The
    lock_for_read() method should be used by threads that only need to read
    the data, like so::

          with storage.lock_for_read(userid, collection):
              ver = storage.get_collection_version(userid, collection)
              if ver <= if_modified_since:
                  raise HTTPNotModified
              return storage.get_items(userid, collection, newer=ver)

    Any thread holding a read lock on a collection is guaranteed to see a
    fixed, consistent view of the data in that collection.  It will be
    isolated from the effects of any concurrent threads attempting to write
    to the storage.

    The lock_for_write() method should be used by threads that need to modify
    the data, like so::

          with storage.lock_for_write(userid, collection):
              ver = storage.get_collection_version(userid, collection)
              if ver > if_unmodified_since:
                  raise HTTPModified
              storage.set_items(userid, collection, new_items)

    There is no guarantee of mutual temporal exclusion between readers and
    writers.  For example, backends that natively support Multi-Version
    Concurrency Control may implement lock_for_read() as a no-op.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def is_healthy(self):
        """Check whether the storage backend is healthy and active."""

    #
    # APIs for collection-level locking.
    #

    def lock_for_read(self, userid, collection):
        """Context manager locking the storage for consistent reads.

        Args:
            userid: integer identifying the user in the storage.
            collection: the name of the collection to lock.

        Returns:
            A context manager that will acquire and release the lock.

        Raises:
            CollectionNotFoundError: the user has no such collection.
        """

    def lock_for_write(self, userid, collection):
        """Context manager locking the storage for consistent writes.

        Args:
            userid: integer identifying the user in the storage.
            collection: the name of the collection to lock.

        Returns:
            A context manager that will acquire and release the lock.

        Raises:
            CollectionNotFoundError: the user has no such collection.
        """

    #
    # APIs to operate on the entire storage.
    #

    @abc.abstractmethod
    def get_storage_version(self, userid):
        """Returns the last-modified version for the entire storage.

        Args:
            userid: integer identifying the user in the storage.

        Returns:
            The last-modified version for the entire storage.
        """

    @abc.abstractmethod
    def get_collection_versions(self, userid):
        """Returns the collection versions for a user.

        Args:
            userid: integer identifying the user in the storage.

        Returns:
            A dict mapping collection names to their last-modified version.
        """

    @abc.abstractmethod
    def get_collection_counts(self, userid):
        """Returns the collection counts.

        Args:
            userid: integer identifying the user in the storage.

        Returns:
            A dict mapping collection names to their item count.
        """

    @abc.abstractmethod
    def get_collection_sizes(self, userid):
        """Returns the total size for each collection.

        Args:
            userid: integer identifying the user in the storage.

        Returns:
            A dict mapping collection names to their total size.
        """

    @abc.abstractmethod
    def get_total_size(self, userid, recalculate=False):
        """Returns the total size a user's stored data.

        Args:
            userid: integer identifying the user in the storage.
            recalculate: whether to recalculate any cached size data.

        Returns:
            The total size in bytes.
        """

    @abc.abstractmethod
    def delete_storage(self, userid):
        """Removes all data for the user.

        Args:
            userid: integer identifying the user in the storage.

        Returns:
            None

        Raises:
            ConflictError: the operation conflicted with a concurrent write.
        """

    #
    # APIs to operate on an individual collection
    #

    @abc.abstractmethod
    def get_collection_version(self, userid, collection):
        """Returns the last-modified version for the named collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.

        Returns:
            The last-modified version for the collection.

        Raises:
            CollectionNotFoundError: the user has no such collection.
        """

    @abc.abstractmethod
    def get_items(self, userid, collection, items=None, older=None,
                  newer=None, limit=None, offset=None, sort=None):
        """Returns items from a collection

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            items: list of strings identifying items to return.
            older: integer; only return items older than this version.
            newer: integer; only return items newer than this version.
            limit: integer; return at most this many items.
            offset: string; an offset vale previously returned as next_offset.
            sort: sort order for results; one of "oldest", "newer" or "index".

        Returns:
            A dict with the following keys:
              items: a list of BSO objects matching the given filters.
              next_offset: a string giving next offset token, if any.

        Raises:
            CollectionNotFoundError: the user has no such collection.
            InvalidOffsetError: the provided offset token is invalid.
        """

    @abc.abstractmethod
    def get_item_ids(self, userid, collection, items=None, older=None,
                     newer=None, limit=None, offset=None, sort=None):
        """Returns item ids from a collection

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            items: list of strings identifying items to return.
            older: integer; only return items older than this version.
            newer: integer; only return items newer than this version.
            limit: integer; return at most this many items.
            offset: string; an offset vale previously returned as next_offset.
            sort: sort order for results; one of "oldest", "newer" or "index".

        Returns:
            A dict with the following keys:
              items: a list of BSO objects matching the given filters.
              next_offset: a string giving next offset token, if any.

        Raises:
            CollectionNotFoundError: the user has no such collection.
            InvalidOffsetError: the provided offset token is invalid.
        """

    @abc.abstractmethod
    def set_items(self, userid, collection, items):
        """Creates or updates multiple items in a collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            items: a list of dicts giving data for each item.

        Returns:
            The new last-modified version for the collection.

        Raises:
            ConflictError: the operation conflicted with a concurrent write.
        """

    @abc.abstractmethod
    def delete_collection(self, userid, collection):
        """Deletes an entire collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.

        Returns:
            The new last-modified version for the storage.

        Raises:
            ConflictError: the operation conflicted with a concurrent write.
            CollectionNotFoundError: the user has no such collection.
        """

    @abc.abstractmethod
    def delete_items(self, userid, collection, items):
        """Deletes multiple items from a collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            items: list of strings identifying the items to be removed.

        Returns:
            The new last-modified version for the collection.

        Raises:
            ConflictError: the operation conflicted with a concurrent write.
            CollectionNotFoundError: the user has no such collection.
        """

    #
    # Items APIs
    #

    @abc.abstractmethod
    def get_item_version(self, userid, collection, item):
        """Returns the last-modified version for the named item.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            item: string identifying the item.

        Returns:
            The last-modified version for the item.

        Raises:
            CollectionNotFoundError: the user has no such collection.
            ItemNotFoundError: the collection contains no such item.
        """

    @abc.abstractmethod
    def get_item(self, userid, collection, item):
        """Returns one item from a collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            item: string identifying the item.

        Returns:
            BSO object corresponding to the item.

        Raises:
            CollectionNotFoundError: the user has no such collection.
            ItemNotFoundError: the collection contains no such item.
        """

    @abc.abstractmethod
    def set_item(self, userid, collection, item, data):
        """Creates or updates a single item in a collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            item: string identifying the item
            data: dict containing the new item data.

        Returns:
            A dict with the following keys:
              created: boolean indicating whether this created a new item
              version: the new last-modified version of the item.

        Raises:
            ConflictError: the operation conflicted with a concurrent write.
        """

    @abc.abstractmethod
    def delete_item(self, userid, collection, item):
        """Deletes a single item from a collection.

        Args:
            userid: integer identifying the user in the storage.
            collection: name of the collection.
            item: string identifying the item

        Returns:
            The new last-modified version for the collection.

        Raises:
            ConflictError: the operation conflicted with a concurrent write.
            CollectionNotFoundError: the user has no such collection.
            ItemNotFoundError: the collection contains no such item.
        """

    @classmethod
    def __subclasshook__(cls, klass):
        for method in cls.__abstractmethods__:
            if any(method in base.__dict__ for base in klass.__mro__):
                continue
            raise TypeError('Missing "%s" in "%s"' % (method, klass))
        if klass not in cls._abc_registry:
            cls._abc_registry.add(klass)
        return True


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


def get_all_storages(config):
    """Iterator over all (hostname, storage) pairs for a config."""
    for key in config.registry:
        if key == "syncstorage:storage:default":
            yield ("default", config.registry[key])
        elif key.startswith("syncstorage:storage:host:"):
            hostname = key[len("syncstorage:storage:host:"):]
            yield (hostname, config.registry[key])


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
        storage = load_storage_from_settings("storage", host_settings)
        config.registry[host_cache_key] = storage
    # Create the default backend to be used by all other hosts.
    storage = load_storage_from_settings("storage", settings)
    config.registry["syncstorage:storage:default"] = storage
    # Scan for additional config from any storage plugins.
    # Some might fail to import, use the onerror callback to ignore them.
    config.scan("syncstorage.storage", onerror=_ignore_import_errors)


def load_storage_from_settings(section_name, settings):
    """Load a SyncStorage backend from the named section of the settings.

    This function lookds in the named section of the given configuration
    settings for details of a SyncStorage backend to create.  The class
    name must be specified by the setting "backend", and other settings will
    be passed to the class constructor as keyword arguments.

    If the settings contain a key named "wraps", this is taken to reference
    another section of the settings from which a subordinate backend plugin
    is loaded.  This allows you to e.g. wrap a MemcachedStorage instance
    around an SQLStorage instance from a single config file.
    """
    section_settings = settings.getsection(section_name)
    klass = resolve_name(section_settings.pop("backend"))
    wraps = section_settings.pop("wraps", None)
    if wraps is None:
        return klass(**section_settings)
    else:
        wrapped_storage = load_storage_from_settings(wraps, settings)
        return klass(wrapped_storage, **section_settings)


def _ignore_import_errors(name):
    """Venusian scan callback that will ignore any ImportError instances."""
    if not issubclass(sys.exc_info()[0], ImportError):
        raise
    logger = get_current_registry()['metlog']
    logger.exception("Error while scanning package %r" % (name,))
