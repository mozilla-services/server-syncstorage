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
import simplejson as json

from webob.exc import (HTTPServiceUnavailable, HTTPBadRequest,
                       HTTPInternalServerError, HTTPNotFound)
from recaptcha.client import captcha

from syncserver.util import (json_response, send_email, valid_email,
                             valid_password, render_mako, raise_503)
from syncserver.respcodes import (WEAVE_MISSING_PASSWORD,
                                   WEAVE_NO_EMAIL_ADRESS,
                                   WEAVE_INVALID_WRITE,
                                   WEAVE_MALFORMED_JSON,
                                   WEAVE_WEAK_PASSWORD,
                                   WEAVE_INVALID_CAPTCHA)

_TPL_DIR = os.path.join(os.path.dirname(__file__), 'templates')


class UserController(object):

    def __init__(self, app, auth):
        self.app = app
        self.auth = raise_503(auth)

    def user_exists(self, request):
        exists = (self.auth.get_user_id(request.sync_info['username'])
                  is not None)
        return json_response(int(exists))

    def user_node(self, request):
        """Returns the storage node root for the user"""
        # XXX the PHP Server does not send a json back here
        # but a plain text expected by the client
        #
        # return json_response(request.host_url)
        return request.host_url + '/'

    def password_reset(self, request):
        """Sends an e-mail for a password reset request."""
        user_id = request.sync_info['user_id']
        code = self.auth.generate_reset_code(user_id)
        user_name, user_email = self.auth.get_user_info(user_id)
        data = {'host': request.host_url, 'user_name': user_name,
                'code': code}
        body = render_mako('password_reset_mail.mako', **data)

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
        except ValueError:
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

        # check if captcha info are provided
        challenge = data.get('captcha-challenge')
        response = data.get('captcha-response')

        if challenge is not None and response is not None:
            resp = captcha.submit(challenge, response,
                                  self.app.config['captcha.private_key'],
                                  remoteip=request.remote_addr)
            if not resp.is_valid:
                raise HTTPBadRequest(WEAVE_INVALID_CAPTCHA)
        else:
            if self.app.config['use_captcha']:
                raise HTTPBadRequest(WEAVE_INVALID_CAPTCHA)

        # all looks good, let's create the user
        if not self.auth.create_user(user_name, password, email):
            raise HTTPInternalServerError('User creation failed.')

        return user_name

    def change_email(self, request):
        """Changes the user e-mail"""
        user_id = request.sync_info['user_id']
        try:
            email = json.loads(request.body)
        except ValueError:
            raise HTTPBadRequest(WEAVE_MALFORMED_JSON)

        if not valid_email(email):
            raise HTTPBadRequest(WEAVE_NO_EMAIL_ADRESS)

        if not self.auth.update_email(user_id, email):
            raise HTTPInternalServerError('User update failed.')

        return email

    def password_reset_form(self, request, **kw):
        """Returns a form for resetting the password"""
        if 'key' in kw:
            # we have a key, let's display the key controlling form
            return render_mako('password_reset_form.mako', **kw)
        elif not request.POST and not request.GET:
            # asking for the first time
            return render_mako('password_ask_reset_form.mako')
        raise HTTPBadRequest()

    def _repost(self, request, error):
        request.POST['error'] = error
        return self.password_reset_form(request, **dict(request.POST))

    def do_password_reset(self, request):
        """Do a password reset."""
        user_name = request.POST.get('username')
        if request.POST.keys() == ['username']:
            # setting up a password reset
            user_name = request.POST['username']
            user_id = self.auth.get_user_id(user_name)
            request.sync_info['user_id'] = user_id
            try:
                self.password_reset(request)
            except HTTPServiceUnavailable, e:
                return render_mako('password_failure.mako', error=e.detail)
            else:
                return render_mako('password_key_sent.mako')
            raise HTTPBadRequest()

        # full form, the actual password reset
        password = request.POST.get('password')
        confirm = request.POST.get('confirm')
        key = request.POST.get('key')

        if user_name is None:
            return self._repost(request,
                                'Username not provided. Please check '
                                'the link you used.')

        user_id = self.auth.get_user_id(user_name)
        if user_id is None:
            return self._repost(request, 'We are unable to locate your '
                                'account')

        if password is None:
            return self._repost(request, 'Password not provided. '
                                'Please check the link you used.')

        if password != confirm:
            return self._repost(request, 'Password and confirmation do '
                                'not match')

        if not valid_password(user_name, password):
            return self._repost(request, 'Password should be at least 8 '
                                'characters and not the same as your '
                                'username')

        if not self.auth.verify_reset_code(user_id, key):
            return self._repost(request, 'Key does not match with username. '
                                'Please request a new key.')

        # everything looks fine
        if not self.auth.update_password(user_id, password):
            return self._repost(request, 'Password change failed '
                                'unexpectedly.')

        self.auth.clear_reset_code(user_id)
        return render_mako('password_changed.mako')

    def delete_user(self, request):
        """Deletes the user."""
        user_id = request.sync_info['user_id']
        res = self.auth.delete_user(user_id)
        return json_response(res)

    def _captcha(self):
        """Return HTML string for inserting recaptcha into a form."""
        return captcha.displayhtml(self.app.config['captcha.public_key'],
                                   use_ssl=self.app.config['captcha.use_ssl'])

    def captcha_form(self, request):
        """Renders the captcha form"""
        if not self.app.config['use_captcha']:
            raise HTTPNotFound('No captcha configured')

        return render_mako('captcha.mako', captcha=self._captcha())
