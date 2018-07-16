# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Pre-built queries for the SQL storage backend.

This module defines a set of pre-built queries for the SQL storage backend.
Each is either a raw SQL string or a function returning an SQLAlchemy
query object.

In addition to standard bindparam syntax, the query loader supports some
string interpolation variables with special meaning:

    * %(bso)s:   insert the name of the user's sharded BSO storage table
    * %(bui)s:   insert the name of the user's sharded batch_upload_items table
    * %(ids)s:   insert a list of items matching the "ids" query parameter.

"""

from sqlalchemy.sql import select, bindparam

# Queries operating on all collections in the storage.

STORAGE_TIMESTAMP = "SELECT MAX(last_modified) FROM user_collections "\
                    "WHERE userid=:userid"

STORAGE_SIZE = "SELECT SUM(payload_size) FROM %(bso)s WHERE "\
               "userid=:userid AND ttl>:ttl"

COLLECTIONS_TIMESTAMPS = "SELECT collection, last_modified "\
                         "FROM user_collections WHERE userid=:userid"

COLLECTIONS_COUNTS = "SELECT collection, COUNT(collection) FROM %(bso)s "\
                     "WHERE userid=:userid AND ttl>:ttl "\
                     "GROUP BY collection"

COLLECTIONS_SIZES = "SELECT collection, SUM(payload_size) FROM %(bso)s "\
                    "WHERE userid=:userid AND ttl>:ttl "\
                    "GROUP BY collection"

DELETE_ALL_BSOS = "DELETE FROM %(bso)s WHERE userid=:userid"

DELETE_ALL_COLLECTIONS = "DELETE FROM user_collections WHERE userid=:userid"

# Queries for locking/unlocking a collection.

BEGIN_TRANSACTION_READ = None

BEGIN_TRANSACTION_WRITE = None

LOCK_COLLECTION_READ = "SELECT last_modified FROM user_collections "\
                       "WHERE userid=:userid AND collection=:collectionid "\
                       "LOCK IN SHARE MODE"

LOCK_COLLECTION_WRITE = "SELECT last_modified FROM user_collections "\
                        "WHERE userid=:userid AND collection=:collectionid "\
                        "FOR UPDATE"

# Queries operating on a particular collection.

COLLECTION_ID = "SELECT collectionid FROM collections "\
                "WHERE name=:name"

COLLECTION_NAME = "SELECT name FROM collections "\
                  "WHERE collectionid=:collectionid"

COLLECTION_NAMES = "SELECT collectionid, name FROM collections "\
                   "WHERE collectionid IN %(ids)s"

# This adds a dummy collection at (:id - 1) so the next autoincr value is :id.
SET_MIN_COLLECTION_ID = "INSERT INTO collections (collectionid, name) "\
                        "VALUES (:collectionid - 1, \"\")"

CREATE_COLLECTION = "INSERT INTO collections (name) "\
                    "VALUES (:name)"

INIT_COLLECTION = "INSERT INTO user_collections "\
                  "(userid, collection, last_modified) "\
                  "VALUES (:userid, :collectionid, :modified)"

TOUCH_COLLECTION = "UPDATE user_collections SET last_modified=:modified "\
                   "WHERE userid=:userid AND collection=:collectionid"

COLLECTION_TIMESTAMP = "SELECT last_modified FROM user_collections "\
                       "WHERE userid=:userid AND collection=:collectionid"

DELETE_COLLECTION_ITEMS = "DELETE FROM %(bso)s WHERE userid=:userid "\
                          "AND collection=:collectionid"

DELETE_COLLECTION = "DELETE FROM user_collections WHERE userid=:userid "\
                    "AND collection=:collectionid"

DELETE_ITEMS = "DELETE FROM %(bso)s WHERE userid=:userid "\
               "AND collection=:collectionid AND id IN %(ids)s"

CREATE_BATCH = "INSERT INTO batch_uploads (batch, userid, collection) "\
                     "VALUES (:batch, :userid, :collection)"

VALID_BATCH = "SELECT batch FROM batch_uploads WHERE batch = :batch " \
                    "AND userid = :userid AND collection = :collection"

# The semantics we want for applying a batch are roughly
# those of an UPSERT, but there's no good generic way
# to do that.  This is a best-effort, inefficient fallback
# using only portable SQL syntax, that applies the batch in two
# parts:
#
#  * Update any existing rows in-place, using subselects to
#    access the data for each such row found.
#  * Insert any new rows with a single INSERT ... SELECT.
#
# Yes, it's horrible, and made even more so due to the need to deal
# with partial updates.  For production use we expect a db-specific
# reimplementation that can do this in a single efficient query.

APPLY_BATCH_UPDATE = """
    UPDATE %(bso)s
    SET
        sortindex = COALESCE(
            (SELECT sortindex FROM %(bui)s WHERE
                batch = :batch AND userid = :userid AND id = %(bso)s.id),
            %(bso)s.sortindex
        ),
        payload = COALESCE(
            (SELECT payload FROM %(bui)s WHERE
                batch = :batch AND userid = :userid AND id = %(bso)s.id),
            %(bso)s.payload,
            ''
        ),
        payload_size = COALESCE(
            (SELECT payload_size FROM %(bui)s WHERE
                batch = :batch AND userid = :userid AND id = %(bso)s.id),
            %(bso)s.payload_size,
            0
        ),
        ttl = COALESCE(
            (SELECT ttl_offset + :ttl_base FROM %(bui)s WHERE
                batch = :batch AND userid = :userid AND id = %(bso)s.id),
            %(bso)s.ttl,
            :default_ttl
        ),
        modified = :modified
    WHERE
        userid = :userid AND
        collection = (
            SELECT collection FROM batch_uploads
            WHERE batch = :batch AND userid = :userid
        ) AND
        id IN (
            SELECT id FROM %(bui)s WHERE batch = :batch AND userid = :userid
        )
"""

APPLY_BATCH_INSERT = """
    INSERT INTO %(bso)s
        (userid, collection, id, sortindex, payload,
        payload_size, ttl, modified)
    SELECT
       batch_uploads.userid,
       batch_uploads.collection,
       %(bui)s.id,
       %(bui)s.sortindex,
       COALESCE(%(bui)s.payload, ''),
       COALESCE(%(bui)s.payload_size, 0),
       COALESCE(%(bui)s.ttl_offset + :ttl_base, :default_ttl),
       :modified
    FROM batch_uploads
    LEFT JOIN %(bui)s
    ON
        %(bui)s.batch = batch_uploads.batch AND
        %(bui)s.userid = batch_uploads.userid
    WHERE
        batch_uploads.batch = :batch AND
        batch_uploads.userid = :userid AND
        %(bui)s.id NOT IN (
            SELECT id
            FROM %(bso)s
            WHERE
                userid = batch_uploads.userid AND
                collection = batch_uploads.collection
        )
"""

CLOSE_BATCH = """
    DELETE FROM batch_uploads
    WHERE batch = :batch AND userid = :userid AND collection = :collection
"""

CLOSE_BATCH_ITEMS = """
    DELETE FROM %(bui)s
    WHERE batch = :batch AND userid = :userid
"""


def FIND_ITEMS(bso, params):
    """Item search query.

    Unlike all the other pre-built queries, this one really can't be written
    as a simple string.  We need to include/exclude various WHERE clauses
    based on the values provided at runtime.
    """
    fields = params.get("fields", None)
    if fields is None:
        query = select([bso])
    else:
        query = select([bso.c[field] for field in fields])
    query = query.where(bso.c.userid == bindparam("userid"))
    query = query.where(bso.c.collection == bindparam("collectionid"))
    # Filter by the various query parameters.
    if "ids" in params:
        # Sadly, we can't use a bindparam in an "IN" expression.
        query = query.where(bso.c.id.in_(params.get("ids")))
    if "newer" in params:
        query = query.where(bso.c.modified > bindparam("newer"))
    if "newer_eq" in params:
        query = query.where(bso.c.modified >= bindparam("newer_eq"))
    if "older" in params:
        query = query.where(bso.c.modified < bindparam("older"))
    if "older_eq" in params:
        query = query.where(bso.c.modified <= bindparam("older_eq"))
    if "ttl" in params:
        query = query.where(bso.c.ttl > bindparam("ttl"))
    # Sort it in the order requested.
    # We always sort by *something*, so that limit/offset work consistently.
    # The default order is by timestamp, which is efficient due to the index.
    # We also want to sort by "id" as a secondary column to ensure a consistent
    # total ordering, which is important when paginating large requests.
    # Since "id" is in the primary key, that *should* be efficient...
    sort = params.get("sort", None)
    if sort == 'index':
        order_args = [bso.c.sortindex.desc(), bso.c.id.desc()]
    elif sort == 'oldest':
        order_args = [bso.c.modified.asc(), bso.c.id.asc()]
    else:
        order_args = [bso.c.modified.desc(), bso.c.id.asc()]
    # ...but unfortunately, sorting by "id" causes a significant slowdown on
    # older versions of MySQL, so it's disabled by default and must be
    # explicitly opted in to via config.
    if not params.get("force_consistent_sort_order", False):
        order_args.pop()
    query = query.order_by(*order_args)
    # Apply limit and/or offset.
    limit = params.get("limit", None)
    if limit is not None:
        query = query.limit(limit)
    offset = params.get("offset", None)
    if offset is not None:
        query = query.offset(offset)
    return query


# Queries operating on a particular item.

DELETE_ITEM = "DELETE FROM %(bso)s WHERE userid=:userid AND "\
              "collection=:collectionid AND id=:item AND ttl>:ttl"\

ITEM_DETAILS = "SELECT id, sortindex, modified, payload "\
               "FROM %(bso)s WHERE collection=:collectionid "\
               "AND userid=:userid AND id=:item AND ttl>:ttl"

ITEM_TIMESTAMP = "SELECT modified FROM %(bso)s "\
                 "WHERE collection=:collectionid AND userid=:userid "\
                 "AND id=:item AND ttl>:ttl"

# Administrative queries

# This query nominally deletes *some* expired items, but not necessarily all.
# The idea is to delete them in small batches to keep overhead low.
# Unfortunately there's no generic way to achieve this in SQL so the default
# case winds up deleting all expired items.  There is a MySQL-specific
# version using DELETE <ttlblah> LIMIT 1000.

PURGE_SOME_EXPIRED_ITEMS = """
    DELETE FROM %(bso)s
    WHERE ttl < (:now - :grace)
"""

PURGE_BATCHES = """
    DELETE FROM batch_uploads
    WHERE batch < (:now - :lifetime - :grace) * 1000
"""

PURGE_BATCH_CONTENTS = """
    DELETE FROM %(bui)s
    WHERE batch < (:now - :lifetime - :grace) * 1000
"""
