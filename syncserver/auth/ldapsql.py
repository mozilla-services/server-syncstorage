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
""" LDAP Authentication
"""
from hashlib import sha1
import random
import datetime
import time
from contextlib import contextmanager

import ldap
from ldap.ldapobject import ReconnectLDAPObject
from ldap.modlist import addModlist

from sqlalchemy.ext.declarative import declarative_base, Column
from sqlalchemy import Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.sql import bindparam, select, insert, delete

from syncserver.util import generate_reset_code, check_reset_code, ssha

_Base = declarative_base()


class ResetCodes(_Base):
    __tablename__ = 'reset_codes'

    username = Column(String(32), primary_key=True, nullable=False)
    reset = Column(String(32))
    expiration = Column(DateTime())

reset_codes = ResetCodes.__table__


class UserIds(_Base):
    __tablename__ = 'user_ids'

    id = Column(Integer, primary_key=True, autoincrement=True)

userids = UserIds.__table__


tables = [reset_codes, userids]

_USER_RESET_CODE = select([reset_codes.c.expiration,
                           reset_codes.c.reset],
              reset_codes.c.username == bindparam('user_name'))


class MaxTriesReachedError(Exception):
    pass


class StateConnector(ReconnectLDAPObject):

    def simple_bind_s(self, who='', cred='', serverctrls=None,
                      clientctrls=None):
        res = ReconnectLDAPObject.simple_bind_s(self, who, cred, serverctrls,
                                                clientctrls)
        self.connected = True
        self.who = who
        return res

    def bind(self, who, cred, method):
        self.who = who
        res = ReconnectLDAPObject.bind(self, who, cred, method)
        self.connected = True
        return res

    def unbind(self, *args, **kw):
        res = ReconnectLDAPObject.unbind(self, *args, **kw)
        self.connected = False
        self.who = None
        return res

    def search_s(self, *args, **kw):
        try:
            return ReconnectLDAPObject.search_s(self, *args, **kw)
        except ldap.SERVER_DOWN:
            # we want to get rid of this connector
            self.unbind()
            raise
        except ldap.NO_SUCH_OBJECT:
            pass
        except Exception:
            # XXX Need to catch more of these
            raise


class ConnectionPool(object):

    def __init__(self, uri, bind=None, passwd=None, size=100, max_tries=3):
        self._pool = []
        self.size = size
        self.max_tries = max_tries
        self.uri = uri
        self.bind = bind
        self.passwd = passwd

    def _get_connection(self, bind=None, passwd=None):
        for conn in self._pool:
            if not conn.active and (conn.who is None or conn.who == bind):
                conn.active = True
                return conn
        if len(self._pool) >= self.size:
            return None
        if bind is None:
            bind = self.bind
        if passwd is None:
            passwd = self.passwd

        try:
            conn = StateConnector(self.uri)
            if bind is not None:
                conn.simple_bind_s(bind, passwd)
        except Exception:
            return None

        conn.active = True
        self._pool.append(conn)
        return conn

    def _release_connection(self, connection):
        if not connection.connected:
            # let's recycle it
            self._pool.remove(connection)
        else:
            # can be reused
            connection.active = False

    @contextmanager
    def ldap_connection(self, bind=None, passwd=None):
        conn = None
        try:
            wait = 0.1
            tries = 0
            while tries < self.max_tries:
                conn = self._get_connection(bind, passwd)
                if conn is not None:
                    break
                time.sleep(wait)
                wait *= 2
                tries += 1

            if conn is None:
                raise MaxTriesReachedError(self.uri, self.bind)

            yield conn
        finally:
            if conn is not None:
                self._release_connection(conn)


class LDAPAuth(object):
    """LDAP authentication."""

    def __init__(self, ldapuri, sqluri, use_tls=False, bind_user='binduser',
                 bind_password='binduser', admin_user='adminuser',
                 admin_password='adminuser', users_root='ou=users,dc=mozilla',
                 admin_proxy=None, pool_size=100, pool_recycle=3600,
                 reset_on_return=True):
        self.ldapuri = ldapuri
        self.sqluri = sqluri
        self.bind_user = bind_user
        self.bind_password = bind_password
        self.admin_user = admin_user
        self.admin_password = admin_password
        self.use_tls = use_tls
        self.admin_proxy = None
        self.users_root = users_root
        self.pool = ConnectionPool(ldapuri, bind_user, bind_password)
        kw = {'pool_size': int(pool_size),
              'pool_recycle': int(pool_recycle),
              'logging_name': 'weaveserver'}

        if self.sqluri.startswith('mysql'):
            kw['reset_on_return'] = reset_on_return
        self._engine = create_engine(sqluri, **kw)
        for table in tables:
            table.metadata.bind = self._engine
            table.create(checkfirst=True)

    def _conn(self, bind=None, passwd=None):
        return self.pool.ldap_connection(bind, passwd)

    @classmethod
    def get_name(self):
        """Returns the name of the authentication backend"""
        return 'ldap'

    def _get_dn(self, uid):
        if self.users_root != 'md5':
            return 'uid=%s,%s' % (uid, self.users_root)
        # calculate the md5
        raise NotImplementedError

    def get_user_id(self, user_name):
        """Returns the id for a user name"""
        user_name = str(user_name)   # XXX only ASCII
        # the user id in LDAP is "uidNumber", and the use name is "uid"
        dn = self._get_dn(user_name)

        with self._conn() as conn:
            try:
                res = conn.search_s(dn, ldap.SCOPE_BASE,
                                    attrlist=['uidNumber'])
            except ldap.NO_SUCH_OBJECT:
                return None

        if res is None:
            return None
        return res[0][1]['uidNumber'][0]

    def _get_next_user_id(self):
        """Returns the next user id"""
        # XXX see if we could use back-sql instead to deal with autoinc
        res = self._engine.execute(insert(userids))
        return res.inserted_primary_key[0]

    def create_user(self, user_name, password, email):
        """Creates a user. Returns True on success."""
        user_name = str(user_name)   # XXX only ASCII
        user_id = self._get_next_user_id()
        password_hash = ssha(password)
        key = '%s%s' % (random.randint(0, 9999999), user_name)
        key = sha1(key).hexdigest()

        user = {'cn': user_name,
                'sn': user_name,
                'uid': user_name,
                'uidNumber': str(user_id),
                'primaryNode': 'weave:',
                'rescueNode': 'weave:',
                'userPassword': password_hash,
                'account-enabled': 'Yes',
                'mail': email,
                'mail-verified': key,
                'objectClass': ['dataStore', 'inetOrgPerson']}

        user = addModlist(user)
        dn = self._get_dn(user_name)

        with self._conn() as conn:
            res, __ = conn.add_s(dn, user)

        return res == ldap.RES_ADD

    def authenticate_user(self, user_name, passwd):
        """Authenticates a user given a user_name and password.

        Returns the user id in case of success. Returns None otherwise."""
        dn = self._get_dn(user_name)
        try:
            with self._conn(dn, passwd) as conn:
                user = conn.search_s(dn, ldap.SCOPE_BASE,
                                    attrlist=['uidNumber', 'account-enabled'])
        except Exception:
            # we want to refine the exc
            return None

        if user is None:
            return None

        user = user[0][1]
        if user['account-enabled'][0] != 'Yes':
            return None

        return user['uidNumber'][0]

    def generate_reset_code(self, user_id):
        """Generates a reset code

        Args:
            user_id: user id

        Returns:
            a reset code, or None if the generation failed
        """
        code, expiration = generate_reset_code()
        user_name, __ = self.get_user_info(user_id)

        # XXX : use onupdate when its mysql
        # otherwise an update
        query = delete(reset_codes).where(reset_codes.c.username == user_name)
        self._engine.execute(query)

        query = insert(reset_codes).values(reset=code,
                                           expiration=expiration,
                                           username=user_name)

        res = self._engine.execute(query)

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
        if not check_reset_code(code):
            return False

        user_name, __ = self.get_user_info(user_id)
        res = self._engine.execute(_USER_RESET_CODE, user_name=user_name)
        res = res.fetchone()

        if res.reset is None or res.expiration is None:
            return False

        # XXX SQLALchemy should turn it into a datetime for us
        # but that does not occur with sqlite
        if isinstance(res.expiration, basestring):
            exp = datetime.datetime.strptime(res.expiration,
                                             '%Y-%m-%d %H:%M:%S.%f')
        else:
            exp = res.expiration

        if exp < datetime.datetime.now():
            # expired
            return False

        if res.reset != code:
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
        user_name, __ = self.get_user_info(user_id)
        query = delete(reset_codes)
        query = query.where(reset_codes.c.username == user_name)
        res = self._engine.execute(query)
        return res.rowcount > 0

    def get_user_info(self, user_id):
        """Returns user info

        Args:
            user_id: user id

        Returns:
            tuple: username, email
        """
        if self.users_root != 'md5':
            dn = self.users_root
        else:
            raise NotImplementedError

        with self._conn() as conn:
            res = conn.search_s(dn, ldap.SCOPE_SUBTREE,
                                filterstr='(uidNumber=%s)' % user_id,
                                attrlist=['cn', 'mail'])

        # need to read the result
        if len(res) == 0:
            return None, None

        res = res[0][1]
        return res['cn'][0], res['mail'][0]

    def update_email(self, user_id, email):
        """Change the user e-mail

        Args:
            user_id: user id
            email: new email

        Returns:
            True if the change was successful, False otherwise
        """
        user_name, __ = self.get_user_info(user_id)
        user = [(ldap.MOD_REPLACE, 'mail', [email])]
        dn = self._get_dn(user_name)

        with self._conn() as conn:
            res, __ = conn.modify_ext_s(dn, user)

        return res == ldap.RES_MODIFY

    def update_password(self, user_id, password):
        """Change the user password

        Args:
            user_id: user id
            password: new password

        Returns:
            True if the change was successful, False otherwise
        """
        password_hash = ssha(password)
        user_name, __ = self.get_user_info(user_id)
        user = [(ldap.MOD_REPLACE, 'userPassword', [password_hash])]
        dn = self._get_dn(user_name)

        with self._conn() as conn:
            res, __ = conn.modify_s(dn, user)

        return res == ldap.RES_MODIFY

    def delete_user(self, user_id):
        """Deletes a user

        Args:
            user_id: user id

        Returns:
            True if the deletion was successful, False otherwise
        """
        user_id = str(user_id)
        user_name, __ = self.get_user_info(user_id)
        dn = self._get_dn(user_name)

        with self._conn() as conn:
            try:
                res, __ = conn.delete_s(dn)
            except ldap.NO_SUCH_OBJECT:
                return False

        return res == ldap.RES_DELETE
