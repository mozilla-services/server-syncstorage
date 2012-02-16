# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import time


def get_timestamp(value=None):
    """Transforms a python time value into a SyncStorage timestamp.

    Python uses integer seconds, SyncStorage uses integer milliseconds.
    """
    if value is None:
        value = time.time()
    return int(value * 1000)


def from_timestamp(value):
    """Transforms a SyncStorage timestamp into a python time value.

    Python uses integer seconds, SyncStorage uses integer milliseconds.
    """
    return int(value) / 1000.0
