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
# The Initial Developer of the Original Code is Mozilla Foundation.
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
User controller. Implements all APIs from:

https://wiki.mozilla.org/Labs/Weave/User/1.0/API

"""
import os
import json
from webob.exc import (HTTPServiceUnavailable, HTTPBadRequest,
                       HTTPInternalServerError)

from weaveserver.util import (json_response, send_email, valid_email,
                              valid_password)
from weaveserver.respcodes import (WEAVE_MISSING_PASSWORD,
                                   WEAVE_NO_EMAIL_ADRESS,
                                   WEAVE_INVALID_WRITE,
                                   WEAVE_MALFORMED_JSON,
                                   WEAVE_WEAK_PASSWORD)

_TPL_DIR = os.path.join(os.path.dirname(__file__), 'templates')

class UserController(object):

    def __init__(self, auth):
        self.auth = auth

    def user_exists(self, request):
        exists = (self.auth.get_user_id(request.sync_info['username'])
                  is not None)
        return json_response(exists)

    def user_node(self, request):
        """Returns the storage node root for the user"""
        # XXX the PHP Server does not send a json back here
        # but a plain text expected by the client
        #
        # return json_response(request.host_url)
        return request.host_url

    def password_reset(self, request):
        """Sends an e-mail for a password reset request."""
        user_id = request.sync_info['user_id']
        code = self.auth.generate_reset_code(user_id)

        # getting the email template
        with open(os.path.join(_TPL_DIR, 'password_reset.tpl')) as f:
            template = f.read()

        user_name, user_email = self.auth.get_user_info(user_id)
        body = template % {'host': request.host_url,
                           'user_name': user_name, 'code': code}

        sender = request.config['smtp.sender']
        host = request.config['smtp.host']
        port = int(request.config['smtp.port'])
        user = request.config.get('smtp.user')
        password = request.config.get('smtp.password')

        subject = 'Resetting your Weave password'
        res, msg = send_email(sender, user_email, subject, body, host, port,
                              user, password)

        if not res:
            raise HTTPServiceUnavailable(msg)

        return 'success'

    def create_user(self, request):
        """Creates a user."""
        user_name = request.sync_info['username']

        if self.auth.get_user_id(user_name) is not None:
            raise HTTPBadRequest(WEAVE_INVALID_WRITE)

        try:
            data = json.loads(request.body)
        except ValueError, e:
            raise HTTPBadRequest(WEAVE_MALFORMED_JSON)

        # getting the e-mail
        email = data.get('email')
        if not valid_email(email):
            raise HTTPBadRequest(WEAVE_NO_EMAIL_ADRESS)

        # getting the password
        password = data.get('password')
        if password is None:
            raise HTTPBadRequest(WEAVE_MISSING_PASSWORD)

        if not valid_password(user_name, password):
            raise HTTPBadRequest(WEAVE_WEAK_PASSWORD)

        # all looks good, let's create the user
        if not self.auth.create_user(user_name, password, email):
            raise HTTPInternalServerError('User creation failed.')

        return user_name
