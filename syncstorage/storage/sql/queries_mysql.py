# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Custom queries for MySQL.

This module overrides some queries from queries_generic.py with code
tailored to MySQL.
"""

# MySQL's non-standard DELETE ORDER BY LIMIT is incredibly useful here.

PURGE_SOME_EXPIRED_ITEMS = "DELETE FROM %(bso)s "\
                           "WHERE ttl < (UNIX_TIMESTAMP() - :grace) "\
                           "ORDER BY ttl LIMIT :maxitems"
