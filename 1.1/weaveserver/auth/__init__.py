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
""" Authentication tool
"""
import abc
from weaveserver.plugin import Plugin


class WeaveAuth(Plugin):
    """Abstract Base Class for the authentication APIs."""
    name = 'auth'

    @abc.abstractmethod
    def authenticate_user(self, username, password):
        """Authenticates a user.

        Args:
            username: string
            password: password hash

        Returns:
            The user id in case of success. None otherwise.
        """

    @abc.abstractmethod
    def generate_reset_code(self, user_id):
        """Generates a reset code

        Args:
            user_id: user id

        Returns:
            a reset code
        """

    @abc.abstractmethod
    def verify_reset_code(self, user_id, code):
        """Generates a reset code

        Args:
            user_id: user id
            code: reset code

        Returns:
            True or False
        """

    @abc.abstractmethod
    def clear_reset_code(self, user_id):
        """Clears the reset code

        Args:
            user_id: user id

        Returns:
            None
        """

    @abc.abstractmethod
    def get_user_info(self, user_id):
        """Returns user info

        Args:
            user_id: user id

        Returns:
            tuple: username, email
        """
