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

XXX cost of server-side sha1
XXX cache sha1 + sql
"""
import string
import random
import datetime
import re

from sqlalchemy.ext.declarative import declarative_base, Column
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.sql import text

from weaveserver.auth import WeaveAuth
from weaveserver.util import validate_password

# sharing the same table than the sql storage
from weaveserver.storage.sqlmappers import users

_SQLURI = 'mysql://sync:sync@localhost/sync'
_RE_CODE = re.compile('[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}')

class SQLAuth(object):
    """SQL authentication."""

    def __init__(self, sqluri=_SQLURI):
        self._engine = create_engine(sqluri, pool_size=20)
        users.metadata.bind = self._engine
        users.create(checkfirst=True)

    @classmethod
    def get_name(self):
        """Returns the name of the authentication backend"""
        return 'sql'

    def authenticate_user(self, username, password):
        """Authenticates a user given a username and password.

        Returns the user id in case of success. Returns None otherwise."""
        query = ('select id, password_hash from users '
                 'where username = :username')

        user = self._engine.execute(text(query), username=username).fetchone()
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

        query = ('update users set reset = :code, '
                 'reset_expiration = :expiration '
                 'where id = :user_id')

        res = self._engine.execute(text(query), user_id=user_id, code=code,
                                   expiration=expiration)
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

        query = ('select reset_expiration, reset from users '
                 'where id = :user_id')
        res = self._engine.execute(text(query), user_id=user_id)
        user = res.fetchone()

        if user.reset is None or user.reset_expiration is None:
            return False

        # XXX SQLALchemy should turn it into a datetime for us
        exp = datetime.datetime.strptime(user.reset_expiration,
                                         '%Y-%m-%d %H:%M:%S.%f')

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
            None
        """
        query = ('update users set reset = :code, '
                 'reset_expiration = :expiration '
                 'where id = :user_id')

        code = expiration = None
        self._engine.execute(text(query), user_id=user_id, code=code,
                             expiration=expiration)

    def get_user_info(self, user_id):
        """Returns user info

        Args:
            user_id: user id

        Returns:
            tuple: username, email
        """
        query = ('select username, email from users '
                 'where id = :user_id')
        res = self._engine.execute(text(query), user_id=user_id).fetchone()
        if res is None:
            return None, None

        return res.username, res.email

WeaveAuth.register(SQLAuth)
