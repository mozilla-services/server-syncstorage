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

# MySQL's non-standard ON DUPLICATE KEY UPDATE means we can
# apply a batch efficiently with a single query.

APPLY_BATCH_UPDATE = None

APPLY_BATCH_INSERT = """
    INSERT INTO %(bso)s
        (userid, collection, id, modified, sortindex,
        ttl, payload, payload_size)
    SELECT
        :userid, :collection, id, :modified, sortindex,
        COALESCE(ttl_offset + :ttl_base, :default_ttl),
        COALESCE(payload, ''),
        COALESCE(payload_size, 0)
    FROM %(bui)s
    WHERE batch = :batch
    ON DUPLICATE KEY UPDATE
        modified = :modified,
        sortindex = COALESCE(%(bui)s.sortindex,
                             %(bso)s.sortindex),
        ttl = COALESCE(%(bui)s.ttl_offset + :ttl_base,
                       %(bso)s.ttl),
        payload = COALESCE(%(bui)s.payload,
                           %(bso)s.payload),
        payload_size = COALESCE(%(bui)s.payload_size,
                                %(bso)s.payload_size)
"""
