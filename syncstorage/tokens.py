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
Token controller. A stub until the tokenserver is ready to test against.
"""

import hashlib

from webob.exc import HTTPNotFound

from zope.interface import implements
from repoze.who.interfaces import IAuthenticator
from repoze.who.plugins.vepauth import SignedTokenManager

from services.user import extract_username


class ServicesTokenManager(SignedTokenManager):
    """SignedTokenManager that produces a numeric userid for services.

    This is a little hack to pretend like we've got a tokenserver.  It
    just turns the email address into a unique numeric id by hashing it.
    """

    @classmethod
    def get_user_data(self, email):
        user = {}
        user["username"] = extract_username(email)
        userid_hash = hashlib.md5(user["username"]).hexdigest()
        user["userid"] = int(int(userid_hash, 32) % (2 ** 31))
        return user

    def make_token(self, request, data):
        data.update(self.get_user_data(data["email"]))
        return super(ServicesTokenManager, self).make_token(request, data)


class TestingAuthenticator(object):
    """Authenticator accepting md5(username) as the password.

    Obviously this is only for testing purposes...
    """
    implements(IAuthenticator)

    def authenticate(self, environ, identity):
        username = identity.get("login")
        password = identity.get("password")
        if not username or not password:
            return None
        if hashlib.md5(username).hexdigest() != password:
            return None
        identity.update(ServicesTokenManager.get_user_data(username))
        return username


class TokenController(object):
    """Stub controller for provisioning auth tokens.

    This will eventually go away, replaced by the tokenserver.  For now
    it's here as a stub to let repoze.who.plugins.vepauth provision tokens.
    """

    def __init__(self, app):
        self.app = app

    def get_token(self, request):
        # This is a little yuck, because services.whoauth doesn't support
        # environ["repoze.who.application"].  We need to catch the failed
        # auth and return it ourselves.  When we migrate to pyramid this
        # will be handled automatically.
        try:
            self.app.auth.check(request, {"auth": "True"})
        except Exception:
            if "repoze.who.application" not in request.environ:
                raise
            return request.environ["repoze.who.application"]
        else:
            return HTTPNotFound()
