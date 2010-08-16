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
""" SQL Authentication

Users are stored with digest password (sha1)
"""
import string
import random
import datetime
import re

from sqlalchemy import create_engine
from sqlalchemy.sql import bindparam, select, insert, update, delete

from weaveserver.util import validate_password, ssha
# sharing the same table than the sql storage
from weaveserver.storage.sqlmappers import users

_SQLURI = 'mysql://sync:sync@localhost/sync'
_RE_CODE = re.compile('[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}')

_USER_ID = select([users.c.id], users.c.username == bindparam('user_name'))

_USER_INFO = select([users.c.username, users.c.email],
                    users.c.id == bindparam('user_id'))

_USER_AUTH = select([users.c.id, users.c.password_hash],
                    users.c.username == bindparam('user_name'))

_USER_RESET_CODE = select([users.c.reset_expiration, users.c.reset],
                          users.c.id == bindparam('user_id'))


class SQLAuth(object):
    """SQL authentication."""

    def __init__(self, sqluri=_SQLURI, captcha=False,
                 captcha_public_key=None, captcha_private_key=None,
                 captcha_use_ssl=False):
        self._engine = create_engine(sqluri, pool_size=20)
        users.metadata.bind = self._engine
        users.create(checkfirst=True)
        self.captcha_public_key = captcha_public_key
        self.captcha_private_key = captcha_private_key
        self.captcha_use_ssl = captcha_use_ssl
        self.captcha = captcha

    @classmethod
    def get_name(self):
        """Returns the name of the authentication backend"""
        return 'sql'

    def get_user_id(self, user_name):
        """Returns the id for a user name"""
        user = self._engine.execute(_USER_ID, user_name=user_name).fetchone()
        if user is None:
            return None
        return user.id

    def create_user(self, user_name, password, email):
        """Creates a user. Returns True on success."""
        password_hash = ssha(password)
        query = insert(users).values(username=user_name, email=email,
                                     password_hash=password_hash, status=1)
        res = self._engine.execute(query)
        return res.rowcount == 1

    def authenticate_user(self, user_name, password):
        """Authenticates a user given a user_name and password.

        Returns the user id in case of success. Returns None otherwise."""
        user = self._engine.execute(_USER_AUTH,
                                    user_name=user_name).fetchone()
        if user is None:
            return None

        if validate_password(password, user.password_hash):
            return user.id

    def generate_reset_code(self, user_id):
        """Generates a reset code

        Args:
            user_id: user id
            password: password hash

        Returns:
            a reset code, or None if the generation failed
        """
        chars = string.ascii_uppercase + string.digits

        def _4chars():
            return ''.join([random.choice(chars) for i in range(4)])

        code = '-'.join([_4chars() for i in range(4)])
        expiration = datetime.datetime.now() + datetime.timedelta(hours=6)

        query = update(users).values(id=user_id, reset=code,
                                     reset_expiration=expiration)
        res = self._engine.execute(query.where(users.c.id == user_id))

        if res.rowcount != 1:
            return None  # XXX see if appropriate

        return code

    def verify_reset_code(self, user_id, code):
        """Verify a reset code

        Args:
            user_id: user id
            code: reset code

        Returns:
            True or False
        """
        if _RE_CODE.match(code) is None:
            return False

        res = self._engine.execute(_USER_RESET_CODE, user_id=user_id)
        user = res.fetchone()

        if user.reset is None or user.reset_expiration is None:
            return False

        # XXX SQLALchemy should turn it into a datetime for us
        # but that does not occur with sqlite
        if isinstance(user.reset_expiration, basestring):
            exp = datetime.datetime.strptime(user.reset_expiration,
                                             '%Y-%m-%d %H:%M:%S.%f')
        else:
            exp = user.reset_expiration

        if exp < datetime.datetime.now():
            # expired
            return False

        if user.reset != code:
            # wrong code
            return False

        return True

    def clear_reset_code(self, user_id):
        """Clears the reset code

        Args:
            user_id: user id

        Returns:
            True if the change was successful, False otherwise
        """
        query = update(users).where(users.c.id == user_id)
        code = expiration = None
        res = self._engine.execute(query.values(id=user_id, reset=code,
                                                reset_expiration=expiration))
        return res.rowcount == 1

    def get_user_info(self, user_id):
        """Returns user info

        Args:
            user_id: user id

        Returns:
            tuple: username, email
        """
        res = self._engine.execute(_USER_INFO, user_id=user_id).fetchone()
        if res is None:
            return None, None

        return res.username, res.email

    def update_email(self, user_id, email):
        """Change the user e-mail

        Args:
            user_id: user id
            email: new email

        Returns:
            True if the change was successful, False otherwise
        """
        query = update(users).where(users.c.id == user_id)
        res = self._engine.execute(query.values(email=email))
        return res.rowcount == 1

    def update_password(self, user_id, password):
        """Change the user password

        Args:
            user_id: user id
            password: new password

        Returns:
            True if the change was successful, False otherwise
        """
        password_hash = ssha(password)
        query = update(users).where(users.c.id == user_id)
        res = self._engine.execute(query.values(password_hash=password_hash))
        return res.rowcount == 1

    def delete_user(self, user_id):
        """Deletes a user

        Args:
            user_id: user id

        Returns:
            True if the deletion was successful, False otherwise
        """
        query = delete(users).where(users.c.id == user_id)
        res = self._engine.execute(query)
        return res.rowcount == 1
