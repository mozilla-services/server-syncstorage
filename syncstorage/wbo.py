# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
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

        try:
            data_items = data.items()
        except AttributeError:
            msg = "WBO data must be dict-like, not %s"
            raise ValueError(msg % (type(data),))

        for name, value in data_items:
            if value is not None:
                if not isinstance(value, (int, long, float, basestring)):
                    msg = "WBO fields must be scalar values, not %s"
                    raise ValueError(msg % (type(value),))
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

        # Check that the payload is a string, and is not too big.
        payload = self.get('payload')
        if payload is not None:
            if not isinstance(payload, basestring):
                return False, 'payload not a string'
            if len(payload.encode("utf8")) > MAX_PAYLOAD_SIZE:
                return False, 'payload too large'

        return True, None
