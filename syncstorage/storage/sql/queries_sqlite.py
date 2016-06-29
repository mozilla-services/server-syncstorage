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

PURGE_SOME_EXPIRED_ITEMS = "DELETE FROM %(bso)s "\
                           "WHERE ttl < (strftime('%%s', 'now') - :grace) "

PURGE_BATCHES = "DELETE FROM batch_uploads WHERE batch < " \
                "   (SELECT strftime('%%s', 'now') - :grace) * 1000"

PURGE_BATCH_CONTENTS = "DELETE FROM %(bui)s WHERE batch < " \
                       "(SELECT strftime('%%s', 'now') - :grace) * 1000"
