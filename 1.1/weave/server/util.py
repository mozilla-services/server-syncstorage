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
Various utilities
"""
import base64
import json
import struct

from webob.exc import HTTPUnauthorized, HTTPBadRequest
from webob import Response


def _normalize(path):
    """Remove extra '/'s"""
    if path[0] == '/':
        path = path[1:]
    return path.replace('//', '/')

# various authorization header names, depending on the setup
_AUTH_HEADERS = ('Authorization', 'AUTHORIZATION', 'HTTP_AUTHORIZATION',
                 'REDIRECT_HTTP_AUTHORIZATION')


def authenticate_user(request, authtool):
    """Authenticate user and extract information from a storage request.

    "request" is the request received, "authtool" is the authentication tool
    that will be used to authenticate the user from the request.

    From the path:
      - api version
      - user name
      - function name
      - params

    From the headers:
      - user name
      - password

    The function makes sure that the user name found in the headers
    is compatible with the request path, then calls the authentication tool
    that returns the user id from the database, if the password is the right
    one.
    """
    path = _normalize(request.path_info)
    paths = path.split('/')

    # basically, any call that does not match an API
    # XXX see if we need to support thoses
    if len(path) <= 3:
        res = {}
    else:
        res = {'api': paths[0], 'username': paths[1], 'function': paths[2],
               'params': paths[3:]}

    # authenticating, if REMOTE_USER is not present in the environ
    if 'REMOTE_USER' not in request.environ:
        auth = None
        for auth_header in _AUTH_HEADERS:
            if auth_header in request.environ:
                auth = request.environ[auth_header]
                break

        if auth is not None:
            # for now, only supporting basic authentication
            # let's decipher the base64 encoded value
            if not auth.startswith('Basic '):
                raise HTTPUnauthorized('Invalid token')

            auth = auth.split('Basic ')[-1].strip()
            user_name, password = base64.decodestring(auth).split(':')

            # let's reject the call if the url is not owned by the user
            if (res.get('username') is not None
                and user_name != res['username']):
                raise HTTPUnauthorized


            # let's try an authentication
            user_id = authtool.authenticate_user(user_name, password)
            if user_id is None:
                raise HTTPUnauthorized

            # we're all clear ! setting up REMOTE_USER and user_id
            request.environ['REMOTE_USER'] = user_name
            res['userid'] = user_id
    else:
        # how do we get the user id in this case ?
        pass

    return res


def json_response(lines):
    """Returns Response containing a json string"""
    return Response(json.dumps(lines), content_type='application/json')


def newlines_response(lines):
    """Returns a Response object containing a newlines output."""

    def _convert(line):
        line = json.dumps(line).replace('\n', '\u000a')
        return '%s\n' % line

    data = [_convert(line) for line in lines]
    return Response(''.join(data), content_type='application/newlines')


def whoisi_response(lines):
    """Returns a Response object containing a whoisi output."""

    def _convert(line):
        line = json.dumps(line)
        size = struct.pack('!I', len(line))
        return '%s%s' % (size, line)

    data = [_convert(line) for line in lines]
    return Response(''.join(data), content_type='application/whoisi')


def convert_response(request, lines):
    """Returns the response in the appropriate format, depending on the accept
    request."""
    accept = request.headers.get('Accept', 'application/json')
    if accept == 'application/json':
        return json_response(lines)
    elif accept == 'application/newlines':
        return newlines_response(lines)
    elif accept == 'application/whoisi':
        return whoisi_response(lines)

    raise HTTPBadRequest('Unsupported format "%s"' % accept)


def check_wbo(data):
    for field in ('parentid', 'id', 'predecessorid'):
        if field not in data:
            continue
        if len(str(data[field])) > 64:
            return False, 'invalid %s' % field

    for field in ('sortindex',):
        if field not in data:
            continue
        try:
            data[field] = int(data[field])
        except ValueError:
            try:
                new = float(data[field])
            except ValueError:
                return False, 'invalid %s' % field
            else:
                data[field] = int(new)

        if data[field] > 999999999 or data[field] < -999999999:
            return False, 'invalid %s' % field

    return True, None
