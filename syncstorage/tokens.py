# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Token controller.  A stub until the tokenserver is ready to test against.
"""

import hashlib

from zope.interface import implements
from repoze.who.interfaces import IAuthenticator
from repoze.who.plugins.vepauth import SignedTokenManager


class ServicesTokenManager(SignedTokenManager):
    """SignedTokenManager that produces a numeric userid for services.

    This is a little hack to pretend like we've got a tokenserver.  It
    just turns the email address into a unique numeric id by hashing it.
    """

    @classmethod
    def get_user_data(self, email):
        user = {}
        userid_hash = hashlib.md5(email).hexdigest()
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
        login = identity.get("login")
        password = identity.get("password")
        if not login or not password:
            return None
        if hashlib.md5(login).hexdigest() != password:
            return None
        identity.update(ServicesTokenManager.get_user_data(login))
        return login
