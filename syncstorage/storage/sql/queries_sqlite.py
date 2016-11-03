# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Custom queries for SQLite.

This module overrides some queries from queries_generic.py with code
tailored to SQLite.
"""

# Queries for locking/unlocking a collection.

BEGIN_TRANSACTION_READ = "BEGIN DEFERRED TRANSACTION"

BEGIN_TRANSACTION_WRITE = "BEGIN EXCLUSIVE TRANSACTION"

LOCK_COLLECTION_READ = "SELECT last_modified FROM user_collections "\
                       "WHERE userid=:userid AND collection=:collectionid"

LOCK_COLLECTION_WRITE = "SELECT last_modified FROM user_collections "\
                        "WHERE userid=:userid AND collection=:collectionid"

# Use the correct timestamp-handling functions for sqlite.

PURGE_SOME_EXPIRED_ITEMS = "DELETE FROM %(bso)s "\
                           "WHERE ttl < (strftime('%%s', 'now') - :grace) "

PURGE_BATCHES = "DELETE FROM batch_uploads WHERE batch < " \
                "   (SELECT strftime('%%s', 'now') - :grace) * 1000"

PURGE_BATCH_CONTENTS = "DELETE FROM %(bui)s WHERE batch < " \
                       "(SELECT strftime('%%s', 'now') - :grace) * 1000"

# We can use INSERT OR REPLACE to apply a batch in a single query.
# However, to correctly cope with with partial data udpates, we need
# to join onto the original table in the SELECT clause so that we
# can coalesce with the existing values.

APPLY_BATCH_UPDATE = None

APPLY_BATCH_INSERT = """
    INSERT OR REPLACE INTO %(bso)s
        (userid, collection, id, sortindex, payload,
        payload_size, ttl, modified)
    SELECT
       batch_uploads.userid,
       batch_uploads.collection,
       %(bui)s.id,
       COALESCE(%(bui)s.sortindex, existing.sortindex),
       COALESCE(%(bui)s.payload, existing.payload, ''),
       COALESCE(%(bui)s.payload_size, existing.payload_size, 0),
       COALESCE(%(bui)s.ttl, existing.ttl, :default_ttl),
       :modified
    FROM batch_uploads
    LEFT JOIN %(bui)s
    ON
        %(bui)s.batch = batch_uploads.batch
    LEFT OUTER JOIN %(bso)s AS existing
    ON
        existing.userid = batch_uploads.userid AND
        existing.collection = batch_uploads.collection AND
        existing.id = %(bui)s.id
    WHERE
        batch_uploads.batch = :batch
"""
