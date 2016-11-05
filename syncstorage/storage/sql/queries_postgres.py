# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Custom queries for PostgreSQL.

This module overrides some queries from queries_generic.py with code
tailored to PostgreSQL.
"""

# Queries for locking/unlocking a collection.

LOCK_COLLECTION_READ = "SELECT last_modified FROM user_collections "\
                       "WHERE userid=:userid AND collection=:collectionid "\
                       "FOR SHARE"

LOCK_COLLECTION_WRITE = "SELECT last_modified FROM user_collections "\
                        "WHERE userid=:userid AND collection=:collectionid "\
                        "FOR UPDATE"

# Postgres aborts the active transaction when it hits a constraint error.
# The app expects to be able to execute these queries, catch an IntegrityError,
# and keep running.  Since Postgres can't do that, we have to try to ensure
# that no IntegerityError will be raised.

# XXX TODO: how to use default serial value while avoiding an error in case
# of conflict?  For now, a race will just cause a 503 response.
CREATE_COLLECTION = "INSERT INTO collections (collectionid, name) "\
                    "VALUES (DEFAULT, :name)"

INIT_COLLECTION = "INSERT INTO user_collections "\
                  "(userid, collection, last_modified) "\
                  "  SELECT :userid, :collectionid, :modified "\
                  "  WHERE NOT EXISTS "\
                  "    (SELECT 1 FROM user_collections "\
                  "     WHERE userid=:userid AND collection=:collectionid)"

# Postgres uses a special sequence thingamabob to handle auto-increment
# columns, so we need a special way to pin its minimum value.

SET_MIN_COLLECTION_ID = """
DO
$do$
BEGIN
IF
  (SELECT nextval('collections_collectionid_seq'::regclass) < :collectionid)
THEN
  ALTER SEQUENCE collections_collectionid_seq
    MINVALUE :collectionid NO MAXVALUE
    START WITH :collectionid
    RESTART WITH :collectionid;
END IF;
END;
$do$;
""".strip()


# Use correct timestamp functions for postgres.

PURGE_SOME_EXPIRED_ITEMS = """
    DELETE FROM %(bso)s
    WHERE ttl < (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - :grace)
"""

PURGE_BATCHES = """
    DELETE FROM batch_uploads
    WHERE batch < (
        SELECT EXTRACT(EPOCH FROM CURRENT_TIMSTAMP) - :lifetime - :grace
    ) * 1000
"""

PURGE_BATCH_CONTENTS = """
    DELETE FROM %(bui)s
    WHERE batch < (
        SELECT EXTRACT(EPOCH FROM CURRENT_TIMSTAMP) - :lifetime - :grace
    ) * 1000
"""
