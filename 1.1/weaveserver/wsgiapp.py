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
import time

from paste.translogger import TransLogger
from paste.exceptions.errormiddleware import ErrorMiddleware

from repoze.profile.profiler import AccumulatingProfileMiddleware
from routes import Mapper

from webob.dec import wsgify
from webob.exc import HTTPNotFound, HTTPUnauthorized, HTTPBadRequest
from webob import Response

from weaveserver import API_VERSION
from weaveserver.util import authenticate_user, convert_config
from weaveserver.storage import WeaveStorage
from weaveserver.auth import WeaveAuth
from weaveserver.controllers.storage import StorageController
from weaveserver.controllers.user import UserController
from weaveserver.controllers.static import StaticController

# URL dispatching happens here
# methods / match / controller / controller method / auth ?

# _API_ is replaced by API_VERSION
# _COLLECTION_ is replaced by {collection:[a-zA-Z0-9._-]+}
# _USERNAME_ is replaced by {username:[a-zA-Z0-9._-]+}
# _ITEM_ is replaced by {item:[a-zA-Z0-9._-]+}

URLS = [('GET', '/', 'storage', 'index', True),

        # storage API
        ('GET', '/_API_/_USERNAME_/info/collections',
         'storage', 'get_collections_info', True),
        ('GET', '/_API_/_USERNAME_/info/collection_counts',
         'storage', 'get_collections_count', True),
        ('GET', '/_API_/_USERNAME_/info/quota', 'storage', 'get_quota', True),
        ('GET', '/_API_/_USERNAME_/info/collections_usage', 'storage',
         'get_collection_sizes', True),
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
         True),

        # user API
        ('GET', '/user/_API_/_USERNAME_', 'user', 'user_exists', False),
        ('PUT', '/user/_API_/_USERNAME_', 'user', 'create_user', False),
        ('DELETE', '/user/_API_/_USERNAME_', 'user', 'delete_user', True),
        ('GET', '/user/_API_/_USERNAME_/node/weave', 'user', 'user_node',
         False),
        ('GET', '/user/_API_/_USERNAME_/password_reset', 'user',
         'password_reset', True),
        ('POST', '/user/_API_/_USERNAME_/email', 'user', 'change_email',
         True),
        ('GET', '/weave-password-reset', 'user', 'password_reset_form', False),
        ('POST', '/weave-password-reset', 'user', 'do_password_reset', False),
        (('GET', 'POST'), '/misc/_API_/captcha_html', 'user', 'captcha_form',
         False),

        # media   XXX served by Apache in real production
        ('GET', '/media/{filename}', 'static', 'get_file', False)]


class SyncServerApp(object):
    """ SyncServerApp dispatches the request to the right controller
    by using Routes.
    """

    def __init__(self, config=None):
        self.mapper = Mapper()
        if config is not None:
            self.config = config
        else:
            self.config = {}

        # loading authentication and storage backends
        self.authtool = WeaveAuth.get_from_config(self.config)
        self.storage = WeaveStorage.get_from_config(self.config)

        # loading and connecting controllers
        self.controllers = {'storage': StorageController(self.storage),
                            'user': UserController(self.authtool),
                            'static': StaticController()}

        for verbs, match, controller, method, auth in URLS:
            if isinstance(verbs, str):
                verbs = [verbs]
            for pattern, replacer in (('_API_', API_VERSION),
                                      ('_COLLECTION_',
                                       '{collection:[a-zA-Z0-9._-]+}'),
                                      ('_USERNAME_',
                                       '{username:[a-zA-Z0-9._-]+}'),
                                      ('_ITEM_',
                                       '{item:[a-zA-Z0-9._-]+}')):
                match = match.replace(pattern, replacer)

            self.mapper.connect(None, match, controller=controller,
                                method=method, conditions=dict(method=verbs),
                                auth=auth)

    @wsgify
    def __call__(self, request):
        if request.method in ('HEAD',):
            raise HTTPBadRequest('"%s" not supported' % request.method)

        request.server_time = float('%.2f' % time.time())
        request.config = self.config

        # removing the trailing slash
        url = request.environ['PATH_INFO'].rstrip('/')
        if url != '':
            request.environ['PATH_INFO'] = url
        match = self.mapper.routematch(environ=request.environ)

        if match is None:
            return HTTPNotFound()

        match, __ = match
        match['api'] = API_VERSION

        if match['auth'] == 'True':
            # needs auth
            user_id = authenticate_user(request, self.authtool,
                                        match.get('username'))
            if user_id is None:
                raise HTTPUnauthorized

            match['user_id'] = user_id

        function = self._get_function(match['controller'], match['method'])
        if function is None:
            raise HTTPNotFound('Unkown URL %r' % request.path_info)

        # extracting all the info from the headers and the url
        request.sync_info = match
        request.config = self.config

        if request.method in ('GET', 'DELETE'):
            # XXX DELETE fills the GET dict.
            params = dict(request.GET)
        else:
            params = {}

        result = function(request, **params)

        if isinstance(result, basestring):
            response = Response(result)
        else:
            # result is already a Response
            response = result

        # setting up the X-Weave-Timestamp
        response.headers['X-Weave-Timestamp'] = str(request.server_time)
        return response

    def _get_function(self, controller, method):
        """Return the method of the right controller."""
        try:
            controller = self.controllers[controller]
        except KeyError:
            return None
        return getattr(controller, method)


def make_app(global_conf, **app_conf):
    """Returns a Sync Server Application."""
    global_conf.update(app_conf)

    params = convert_config(global_conf)
    app = SyncServerApp(params)

    if params.get('translogger', False):
        app = TransLogger(app, logger_name='weaveserver',
                          setup_console_handler=True)

    if params.get('profile', False):
        app = AccumulatingProfileMiddleware(app,
                                         log_filename='profile.log',
                                         cachegrind_filename='cachegrind.out',
                                         discard_first_request=True,
                                         flush_at_shutdown=True,
                                         path='/__profile__')

    if params.get('debug', False):
        app = ErrorMiddleware(app, debug=True,
                              show_exceptions_in_wsgi_errors=True)

    return app
