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

from services.baseapp import set_app, SyncServerApp, Authentication
from syncstorage.controller import StorageController
from syncstorage.storage import get_storage

try:
    from memcache import Client
except ImportError:
    Client = None       # NOQA

_EXTRAS = {'auth': True}


def _url(url):
    for pattern, replacer in (('_API_', '{api:1.0|1|1.1}'),
                              ('_COLLECTION_',
                               '{collection:[a-zA-Z0-9._-]+}'),
                              ('_USERNAME_',
                               '{username:[a-zA-Z0-9._-]+}'),
                              ('_ITEM_',
                              r'{item:[\\a-zA-Z0-9._?#~-]+}')):
        url = url.replace(pattern, replacer)
    return url


urls = [('GET', _url('/_API_/_USERNAME_/info/collections'),
         'storage', _url('get_collections'), _EXTRAS),
        ('GET', _url('/_API_/_USERNAME_/info/collection_counts'),
         'storage', 'get_collection_counts', _EXTRAS),
        ('GET', _url('/_API_/_USERNAME_/info/quota'), 'storage', 'get_quota',
          _EXTRAS),
        ('GET', _url('/_API_/_USERNAME_/info/collection_usage'), 'storage',
         'get_collection_usage', _EXTRAS),
        # XXX empty collection call
        ('PUT', _url('/_API_/_USERNAME_/storage/'), 'storage', 'get_storage',
         _EXTRAS),
        ('GET', _url('/_API_/_USERNAME_/storage/_COLLECTION_'), 'storage',
        'get_collection', _EXTRAS),
        ('GET', _url('/_API_/_USERNAME_/storage/_COLLECTION_/_ITEM_'),
         'storage', 'get_item', _EXTRAS),
        ('PUT', _url('/_API_/_USERNAME_/storage/_COLLECTION_/_ITEM_'),
         'storage', 'set_item', _EXTRAS),
        ('POST', _url('/_API_/_USERNAME_/storage/_COLLECTION_'), 'storage',
        'set_collection', _EXTRAS),
        ('PUT', _url('/_API_/_USERNAME_/storage/_COLLECTION_'), 'storage',
        'set_collection', _EXTRAS),
        ('DELETE', _url('/_API_/_USERNAME_/storage/_COLLECTION_'), 'storage',
        'delete_collection', _EXTRAS),
        ('DELETE', _url('/_API_/_USERNAME_/storage/_COLLECTION_/_ITEM_'),
         'storage', 'delete_item', _EXTRAS),
        ('DELETE', _url('/_API_/_USERNAME_/storage'), 'storage',
         'delete_storage', _EXTRAS)]

controllers = {'storage': StorageController}


class StorageServerApp(SyncServerApp):
    """Storage application"""
    def __init__(self, urls, controllers, config=None,
                 auth_class=Authentication):
        super(StorageServerApp, self).__init__(urls, controllers, config,
                                               auth_class)
        # collecting the host-specific config and building connectors
        self.storages = {}
        hosts = config.get('storage.hosts', ['localhost'])
        if 'localhost' not in hosts:
            hosts.append('localhost')
        for host in hosts:
            config = self._host_specific(host, config)
            self.storages[host] = get_storage(config)

        self.config = config
        self.check_blacklist = \
                self.config.get('storage.check_blacklisted_nodes', False)
        if self.check_blacklist and Client is not None:
            servers = self.config.get('storage.cache_servers',
                                      '127.0.0.1:11211')
            self.cache = Client(servers.split(','))
        else:
            if self.check_blacklist:
                raise ValueError('The "check_blacklisted_node" option '
                                 'needs a memcached server')
            self.cache = None

    def get_storage(self, request):
        host = request.host
        if host not in self.storages:
            host = 'localhost'
        return self.storages[host]

    def _before_call(self, request):
        # let's control if this server is not on the blacklist
        if not self.check_blacklist:
            return {}

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

    def _debug_server(self, request):
        res = []
        storage = self.get_storage(request)
        res.append('- backend: %s' % storage.get_name())
        if storage.get_name() in ('memcached',):
            cache_servers = ['%s:%d' % (server.ip, server.port)
                             for server in storage.cache.servers]
            res.append('- memcached servers: %s</li>' % \
                    ', '.join(cache_servers))

        if storage.get_name() in ('sql', 'memcached'):
            res.append('- sqluri: %s' % storage.sqluri)
        return res


make_app = set_app(urls, controllers, klass=StorageServerApp)
