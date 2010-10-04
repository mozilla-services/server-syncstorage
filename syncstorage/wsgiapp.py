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
Application entry point.
"""
from webob.exc import HTTPServiceUnavailable

from synccore.baseapp import set_app, SyncServerApp
from syncstorage.controller import StorageController
from syncstorage.storage import WeaveStorage

# pre-registering storage backend so they are easier to set up
from syncstorage.storage.sql import SQLStorage
WeaveStorage.register(SQLStorage)
try:
    from syncstorage.storage.memcachedsql import MemcachedSQLStorage
    WeaveStorage.register(MemcachedSQLStorage)
except ImportError:
    pass

try:
    from memcache import Client
except ImportError:
    Client = None       # NOQA


urls = [('GET', '/_API_/_USERNAME_/info/collections',
         'storage', 'get_collections', True),
        ('GET', '/_API_/_USERNAME_/info/collection_counts',
         'storage', 'get_collection_counts', True),
        ('GET', '/_API_/_USERNAME_/info/quota', 'storage', 'get_quota', True),
        ('GET', '/_API_/_USERNAME_/info/collection_usage', 'storage',
         'get_collection_usage', True),
        # XXX empty collection call
        ('PUT', '/_API_/_USERNAME_/storage/', 'storage', 'get_storage', True),
        ('GET', '/_API_/_USERNAME_/storage/_COLLECTION_', 'storage',
        'get_collection', True),
        ('GET', '/_API_/_USERNAME_/storage/_COLLECTION_/_ITEM_', 'storage',
        'get_item', True),
        ('PUT', '/_API_/_USERNAME_/storage/_COLLECTION_/_ITEM_', 'storage',
        'set_item', True),
        ('POST', '/_API_/_USERNAME_/storage/_COLLECTION_', 'storage',
        'set_collection', True),
        ('PUT', '/_API_/_USERNAME_/storage/_COLLECTION_', 'storage',  # XXX FT
        'set_collection', True),
        ('DELETE', '/_API_/_USERNAME_/storage/_COLLECTION_', 'storage',
        'delete_collection', True),
        ('DELETE', '/_API_/_USERNAME_/storage/_COLLECTION_/_ITEM_', 'storage',
        'delete_item', True),
        ('DELETE', '/_API_/_USERNAME_/storage', 'storage', 'delete_storage',
         True)]

controllers = {'storage': StorageController}


class StorageServerApp(SyncServerApp):
    """Storage application"""
    def __init__(self, urls, controllers, config=None):
        self.storage = WeaveStorage.get_from_config(config)
        super(StorageServerApp, self).__init__(urls, controllers, config)
        self.check_blacklist = \
                self.config.get('storage.check_blacklisted_nodes', False)
        if self.check_blacklist and Client is not None:
            servers = self.config.get('servers', '127.0.0.1:11211')
            self.cache = Client(servers.split(','))
        else:
            self.cache = None

    def _before_call(self, request):
        # let's control if this server is not on the blacklist
        if not self.check_blacklist:
            return
        host = request.host
        if self.cache.get('down:%s' % host) is not None:
            # the server is marked as down -- let's exit
            raise HTTPServiceUnavailable("Server Problem Detected")

        backoff = self.cache.get('backoff:%s' % host)
        if backoff is not None:
            # the server is marked to back-off requests. We will treat those
            # but add the header
            return {'X-Weave-Backoff': str(backoff)}
        return {}

make_app = set_app(urls, controllers, klass=StorageServerApp)
