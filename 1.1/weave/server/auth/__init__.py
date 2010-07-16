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

_BACKENDS = {}


class WeaveAuthBase(object):
    """Abstract Base Class for the authentication APIs."""
    __metaclass__ = abc.ABCMeta

    @classmethod
    def __subclasshook__(cls, klass):
        if cls is WeaveAuthBase:
            for method in cls.__abstractmethods__:
                if any(method in base.__dict__ for base in klass.__mro__):
                    continue
                return NotImplemented
            return True
        return NotImplemented

    @abc.abstractmethod
    def get_name(self):
        """Returns the name of the authentication backend"""

    @abc.abstractmethod
    def authenticate_user(self, username, password):
        """Authenticates a user given a username and password.

        Returns the user id in case of success. Returns None otherwise."""


def register(klass):
    """Registers a new storage."""
    if not issubclass(klass, WeaveAuthBase):
        raise TypeError('Not an authentication class')

    auth = klass()
    _BACKENDS[auth.get_name()] = auth


def get_auth_tool(name):
    """Returns an authentication tool."""
    # hard-load existing tools
    # XXX see if we want to load them dynamically
    from weave.server.auth import dummy
    return _BACKENDS[name]
