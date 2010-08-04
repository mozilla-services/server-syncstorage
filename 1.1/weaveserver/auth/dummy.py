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
""" Dummy Authentication
"""
from weaveserver.auth import WeaveAuth


class DummyAuth(object):
    """Dummy authentication.

    Will store the user ids in memory"""

    def __init__(self):
        self._users = {}

    @classmethod
    def get_name(self):
        """Returns the name of the authentication backend"""
        return 'dummy'

    def authenticate_user(self, username, password):
        """Authenticates a user given a username and password.

        Returns the user id in case of success. Returns None otherwise."""

        if username in self._users:
            return self._users[username]
        id_ = 1
        ids = self._users.values()
        while id_ in ids:
            id_ += 1
        self._users[username] = id_
        return id_

    def generate_reset_code(self, user_id):
        """Generates a reset code"""
        return 'XXXX-XXXX-XXXX-XXXX'

    def verify_reset_code(self, user_id, code):
        """Verify a reset code"""
        return True

    def clear_reset_code(self, user_id):
        """Clears the reset code"""
        return


WeaveAuth.register(DummyAuth)
