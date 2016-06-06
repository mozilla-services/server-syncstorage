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
                           "WHERE ttl < (UNIX_TIMESTAMP() - :grace) " \
                           "ORDER BY ttl LIMIT :maxitems"

PURGE_BATCH_CONTENTS = "DELETE FROM %(bui)s " \
                       "WHERE batch < (UNIX_TIMESTAMP() - :grace) * 1000 " \
                       "ORDER BY ttl LIMIT :maxitems"

APPLY_BATCH = "INSERT INTO %(bso)s "\
                     "  (userid, collection, id, sortindex, modified, "\
                     "   payload, payload_size, ttl) "\
                     "SELECT "\
                     "  :userid, :collection, id, sortindex, :modified, " \
                     "  COALESCE(payload, \"\"), COALESCE(payload_size, 0), "\
                     "  COALESCE(ttl, ttl + :default_ttl) "\
                     "FROM %(bui)s "\
                     "WHERE batch = :batch "\
                     "ON DUPLICATE KEY UPDATE "\
                     "  sortindex = COALESCE(VALUES(sortindex), " \
                     "                       %(bso)s.sortindex), " \
                     "  modified = :modified, " \
                     "  payload = COALESCE(VALUES(payload), " \
                     "                     %(bso)s.payload), "\
                     "  payload_size = COALESCE(VALUES(payload_size),"\
                     "                          %(bso)s.payload_size), "\
                     "  ttl = COALESCE(VALUES(ttl), %(bso)s.ttl)"
