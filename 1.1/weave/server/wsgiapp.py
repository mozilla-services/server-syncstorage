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
from routes import Mapper, URLGenerator
from webob.dec import wsgify
from webob.exc import HTTPNotFound, HTTPUnauthorized

from weave.server import API_VERSION
from weave.server.util import authenticate_user
from weave.server.storage import get_storage
from weave.server.auth import get_auth_tool

# XXX see if we want to load these dynamically
from weave.server.storage import sql
from weave.server.auth import dummy
from weave.server.storagecontroller import StorageController

# URL dispatching happens here
# methods / match / controller / method
# _API_ is replaced by API_VERSION
URLS = [('GET', '/', 'storage', 'index'),
        ('GET', '/_API_/{username}/info/collections',
         'storage', 'get_collections_info'),
        ('GET', '/_API_/{username}/info/collection_counts',
         'storage', 'get_collections_count'),
        ('GET', '/_API_/{username}/info/quota', 'storage', 'get_quota'),
        ('GET', '/_API_/{username}/storage/{collection}', 'storage',
        'get_collection'),
        ('GET', '/_API_/{username}/storage/{collection}/{item}', 'storage',
        'get_item'),
        ('PUT', '/_API_/{username}/storage/{collection}/{item}', 'storage',
        'set_item'),
        ('POST', '/_API_/{username}/storage/{collection}', 'storage',
        'set_collection'),
        ('DELETE', '/_API_/{username}/storage/{collection}', 'storage',
        'delete_collection'),
        ('DELETE', '/_API_/{username}/storage/{collection}/{item}', 'storage',
        'delete_item'),
        ('DELETE', '/_API_/{username}/storage', 'storage', 'delete_storage')]


class SyncServerApp(object):
    """ SyncServerApp dispatches the request to the right controller
    by using Routes.
    """

    def __init__(self, global_conf, app_conf):
        self.mapper = Mapper()
        self.config = {}
        self.config.update(global_conf)
        self.config.update(app_conf)

        # loading authentication and storage backends
        self.authtool = get_auth_tool(self.config['auth'],
                                      **self._get_params('auth'))

        self.storage = get_storage(self.config['storage'],
                                   **self._get_params('storage'))

        # loading and connecting controllers
        self.controllers = {'storage': StorageController(self.storage)}
        for verbs, match, controller, method in URLS:
            if isinstance(verbs, str):
                verbs = [verbs]
            match = match.replace('_API_', API_VERSION)
            self.mapper.connect(None, match, controller=controller,
                                method=method, conditions=dict(method=verbs))

    def _get_params(self, prefix):
        """Returns options filtered by names starting with 'prefix.'"""
        return dict([(param.split('.')[-1], value)
                      for param, value in self.config.items()
                    if param.startswith(prefix + '.')])

    @wsgify
    def __call__(self, request):
        # XXX All requests in the Sync APIs
        # are authenticated
        request.sync_info = authenticate_user(request, self.authtool)
        if 'userid' not in request.sync_info:
            raise HTTPUnauthorized
        if 'REMOTE_USER' not in request.environ:
            raise HTTPUnauthorized

        match = self.mapper.routematch(environ=request.environ)
        if match is None:
            return HTTPNotFound('Unkwown URL %r' % request.path_info)

        match, __ = match
        function = self._get_function(match['controller'], match['method'])
        if function is None:
            raise HTTPNotFound('Unkown URL %r' % request.path_info)

        # make sure the verb matches

        # extracting all the info from the headers and the url
        request.link = URLGenerator(self.mapper, request.environ)
        request.urlvars = ((), match)
        request.config = self.config

        if request.method in ('GET', 'DELETE'):
            # XXX DELETE fills the GET dict.
            params = dict(request.GET)
        else:
            params = {}
        return function(request, **params)

    def _get_function(self, controller, method):
        """Return the method of the right controller."""
        try:
            controller = self.controllers[controller]
        except KeyError:
            return None
        return getattr(controller, method)


def make_app(global_conf, **app_conf):
    """Returns a Sync Server Application."""
    app = SyncServerApp(global_conf, app_conf)
    return app
