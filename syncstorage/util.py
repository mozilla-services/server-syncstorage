# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import time
import decimal
import simplejson


TWO_DECIMAL_PLACES = decimal.Decimal("1.00")


def get_timestamp(value=None):
    """Transforms a python time value into a syncstorage timestamp."""
    if value is None:
        value = time.time()
    try:
        if not isinstance(value, decimal.Decimal):
            value = decimal.Decimal(str(value))
        return value.quantize(TWO_DECIMAL_PLACES,
                              rounding=decimal.ROUND_CEILING)
    except decimal.InvalidOperation, e:
        raise ValueError(str(e))


def json_dumps(value):
    """Decimal-aware version of json.dumps()."""
    return simplejson.dumps(value, use_decimal=True)


def json_loads(value):
    """Decimal-aware version of json.loads()."""
    return simplejson.loads(value, use_decimal=True)
