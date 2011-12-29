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
""" WBO object -- used for (de)serialization
"""

_FIELDS = set(('id', 'username', 'collection', 'parentid',
               'predecessorid', 'sortindex', 'modified',
               'payload', 'payload_size', 'ttl'))

MAX_TTL = 31536000
MAX_ID_SIZE = 64
MAX_PAYLOAD_SIZE = 256 * 1024
MAX_SORTINDEX_VALUE = 999999999
MIN_SORTINDEX_VALUE = -999999999


class WBO(dict):
    """Holds WBO info"""

    def __init__(self, data=None, converters=None):
        if data is None:
            data = {}
        if converters is None:
            converters = {}

        for name, value in data.items():
            if name not in _FIELDS:
                continue
            if name in converters:
                value = converters[name](value)
            if value is None:
                continue

            self[name] = value

    def validate(self):
        """Validates the values the WBO has."""

        # Check that refs to other WBOs are well-formed.
        for field in ('parentid', 'id', 'predecessorid'):
            if field not in self:
                continue
            value = str(self[field])
            if len(value) > MAX_ID_SIZE:
                return False, 'invalid %s' % field
            self[field] = value

        # Check that the ttl is an int, and less than one year.
        if 'ttl' in self:
            try:
                ttl = int(self['ttl'])
            except ValueError:
                return False, 'invalid ttl'

            if ttl < 0 or ttl > MAX_TTL:
                return False, 'invalid ttl'
            self['ttl'] = ttl

        # Check that sorting fields are valid integers.
        # Convert from other types as necessary.
        for field in ('sortindex',):
            if field not in self:
                continue
            try:
                self[field] = int(self[field])
            except ValueError:
                try:
                    new = float(self[field])
                except ValueError:
                    return False, 'invalid %s' % field

                self[field] = int(new)

            if self[field] > MAX_SORTINDEX_VALUE:
                return False, 'invalid %s' % field
            if self[field] < MIN_SORTINDEX_VALUE:
                return False, 'invalid %s' % field

        # Check that the payload is not too big.
        payload = self.get('payload')
        if payload is not None:
            if len(payload.encode("utf8")) > MAX_PAYLOAD_SIZE:
                return False, 'payload too large'

        return True, None
