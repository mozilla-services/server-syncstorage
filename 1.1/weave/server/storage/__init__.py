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

_BACKENDS = {}


class WeaveStorageBase(object):
    """Abstract Base Class for the storage."""
    __metaclass__ = abc.ABCMeta

    @classmethod
    def __subclasshook__(cls, klass):
        if cls is WeaveStorageBase:
            for method in cls.__abstractmethods__:
                if any(method in base.__dict__ for base in klass.__mro__):
                    continue
                return NotImplemented
            return True
        return NotImplemented

    @abc.abstractmethod
    def get_name(self):
        """Returns the name of the storage"""


    #
    # Users APIs -- the user id is the email
    #

    @abc.abstractmethod
    def user_exists(self, user_name):
        """Returns user infos. user is the key"""

    @abc.abstractmethod
    def set_user(self, user_email, **values):
        """set a users information."""

    @abc.abstractmethod
    def get_user(self, user_name, fields=None):
        """Returns user information.

        If fields is provided, its a list of fields to return
        """

    @abc.abstractmethod
    def delete_user(self, user_name):
        """Deletes all data from a user"""


    #
    # Collections APIs
    #

    @abc.abstractmethod
    def delete_collection(self, user_name, collection_id):
        """deletes a collection"""

    @abc.abstractmethod
    def collection_exists(self, user_name, collection_id):
        """Returns True if the collection exists"""

    @abc.abstractmethod
    def set_collection(self, user_name, collection_id, **values):
        """Creates a new collection."""

    @abc.abstractmethod
    def get_collection(self, user_name, collection_id, fields=None):
        """Return information about a collection."""

    @abc.abstractmethod
    def get_collections(self, user_name, fields=None):
        """returns the collections information """

    @abc.abstractmethod
    def get_collection_names(self, user_name):
        """return the collection names"""

    #
    # Items APIs
    #

    @abc.abstractmethod
    def item_exists(self, user_name, collection_id, item_id):
        """Returns user infos. user is the key"""

    @abc.abstractmethod
    def get_items(self, user_name, collection_id, fields=None):
        """returns items from a collection"""

    @abc.abstractmethod
    def get_item(self, user_name, collection_id, item_id, fields=None):
        """returns one item"""

    @abc.abstractmethod
    def set_item(self, user_name, collection_id, item_id, **values):
        """Sets an item"""

    @abc.abstractmethod
    def delete_item(self, user_name, collection_id, item_id):
        """Deletes an item"""

    @abc.abstractmethod
    def delete_items(self, user_name, collection_id, item_ids=None):
        """Deletes items. All items are removed unless item_ids is provided"""


def register(klass):
    """Registers a new storage."""
    if not issubclass(klass, WeaveStorageBase):

        raise TypeError('Not a storage class')

    storage = klass()
    _BACKENDS[storage.get_name()] = storage


def get_storage(name):
    """Returns a storage."""
    return _BACKENDS[name]
