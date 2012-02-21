# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Token controller.  A stub until the tokenserver is ready to test against.
"""

import hashlib
import base64
import urllib2

from zope.interface import implements
from repoze.who.interfaces import IAuthenticator
from repoze.who.plugins.vepauth import SignedTokenManager


#  These are copy-pasted from server-core, and are here only
#  while we're still using basic-auth and requiring a username.

def email_to_idn(addr):
    """ Convert an UTF-8 encoded email address to it's IDN (punycode)
        equivalent

        this method can raise the following:
        UnicodeError -- the passed string is not Unicode valid or BIDI
        compliant
          Be sure to examine the exception cause to determine the final error.
    """
    # decode the string if passed as MIME (some MIME encodes @)
    addr = urllib2.unquote(addr).decode('utf-8')
    if '@' not in addr:
        return addr
    prefix, suffix = addr.split('@', 1)
    return "%s@%s" % (prefix.encode('idna'), suffix.encode('idna'))


def extract_username(username):
    """Extracts the user name.

    Takes the username and if it is an email address, munges it down
    to the corresponding 32-character username
    """
    if '@' not in username:
        return username
    username = email_to_idn(username).lower()
    hashed = hashlib.sha1(username).digest()
    return base64.b32encode(hashed).lower()


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
