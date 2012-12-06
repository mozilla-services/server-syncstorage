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
    * %(ids)s:   insert a list of items matching the "ids" query parameter.

"""

from sqlalchemy.sql import select, bindparam, and_

from syncstorage.storage.sql.dbconnect import user_collections

# Queries operating on all collections in the storage.

STORAGE_VERSION = "SELECT MAX(last_modified_v) FROM user_collections "\
                  "WHERE userid=:userid"

STORAGE_SIZE = "SELECT SUM(b.payload_size) "\
               "FROM %(bso)s b, user_collections uc "\
               "WHERE b.userid=:userid AND uc.userid=:userid "\
               "AND b.collection=uc.collection "\
               "AND b.ttl>:ttl AND b.version>uc.last_deleted_v"

COLLECTIONS_VERSIONS = "SELECT collection, last_modified_v "\
                       "FROM user_collections "\
                       "WHERE userid=:userid "\
                       "AND last_modified_v > last_deleted_v"\

COLLECTIONS_COUNTS = "SELECT b.collection, COUNT(b.collection) "\
                     "FROM %(bso)s b, user_collections uc "\
                     "WHERE b.userid=:userid AND uc.userid=:userid "\
                     "AND b.collection=uc.collection "\
                     "AND b.ttl>:ttl AND b.version>uc.last_deleted_v "\
                     "GROUP BY b.collection"

COLLECTIONS_SIZES = "SELECT b.collection, SUM(b.payload_size) "\
                    "FROM %(bso)s b, user_collections uc "\
                    "WHERE b.userid=:userid AND uc.userid=:userid "\
                    "AND b.collection=uc.collection "\
                    "AND b.ttl>:ttl AND b.version>uc.last_deleted_v "\
                    "GROUP BY b.collection"

DELETE_ALL_COLLECTIONS = "UPDATE user_collections "\
                         "SET last_modified_v=:version, "\
                         "    last_deleted_v=:version "\
                         "WHERE userid=:userid"

# Queries for locking/unlocking a collection.

BEGIN_TRANSACTION_READ = None

BEGIN_TRANSACTION_WRITE = None

LOCK_COLLECTION_READ = "SELECT last_modified_v FROM user_collections "\
                       "WHERE userid=:userid AND collection=:collectionid "\
                       "AND last_modified_v > last_deleted_v "\
                       "LOCK IN SHARE MODE"

LOCK_COLLECTION_WRITE = "SELECT last_modified_v FROM user_collections "\
                        "WHERE userid=:userid AND collection=:collectionid "\
                        "FOR UPDATE"

# Queries operating on a particular collection.

COLLECTION_ID = "SELECT collectionid FROM collections "\
                "WHERE name=:name"

COLLECTION_NAME = "SELECT name FROM collections "\
                  "WHERE collectionid=:collectionid"

COLLECTION_NAMES = "SELECT collectionid, name FROM collections "\
                   "WHERE collectionid IN %(ids)s"

INSERT_COLLECTION = "INSERT INTO collections (collectionid, name) "\
                    "VALUES (:collectionid, :name)"

INIT_COLLECTION = "INSERT INTO user_collections "\
                  "(userid, collection, last_modified_v, last_deleted_v) "\
                  "VALUES (:userid, :collectionid, :version, 0)"

TOUCH_COLLECTION = "UPDATE user_collections SET last_modified_v=:version "\
                   "WHERE userid=:userid AND collection=:collectionid"

COLLECTION_VERSION = "SELECT last_modified_v FROM user_collections "\
                     "WHERE userid=:userid AND collection=:collectionid "\
                     "AND last_modified_v > last_deleted_v"

COLLECTION_LAST_DELETED = "SELECT last_deleted_v FROM user_collections "\
                          "WHERE userid=:userid AND collection=:collectionid "

DELETE_COLLECTION = "UPDATE user_collections "\
                    "SET last_modified_v=:version, last_deleted_v=:version "\
                    "WHERE userid=:userid AND collection=:collectionid "\
                    "AND last_modified_v > last_deleted_v"

DELETE_ITEMS = "DELETE FROM %(bso)s WHERE userid=:userid "\
               "AND collection=:collectionid AND id IN %(ids)s"


def FIND_ITEMS(bso, params):
    """Item search query.

    Unlike all the other pre-built queries, this one really can't be written
    as a simple string.  We need to include/exclude various WHERE clauses
    based on the values provided at runtime.
    """
    fields = params.pop("fields", None)
    if fields is None:
        query = select([bso])
    else:
        query = select([bso.c[field] for field in fields])
    query = query.where(bso.c.userid == bindparam("userid"))
    query = query.where(bso.c.collection == bindparam("collectionid"))
    # Ensure that it hasn't been deleted by a previous operation.
    # This formulates a sub-query to look up last_deleted_v and then
    # check the the item's version is greater than that.
    # XXX TODO: check if mysql can successfully optimize this sub-query
    last_deleted_v = select([user_collections.c.last_deleted_v]).where(and_(
        user_collections.c.userid == bindparam("userid"),
        user_collections.c.collection == bindparam("collectionid"),
    ))
    query = query.where(bso.c.version > last_deleted_v)
    # Filter by the various query parameters.
    if "ids" in params:
        # Sadly, we can't use a bindparam in an "IN" expression.
        query = query.where(bso.c.id.in_(params.pop("ids")))
    if "older" in params:
        query = query.where(bso.c.version < bindparam("older"))
    if "newer" in params:
        query = query.where(bso.c.version > bindparam("newer"))
    if "ttl" in params:
        query = query.where(bso.c.ttl > bindparam("ttl"))
    # Sort it in the order requested.
    # We always sort by *something*, so that limit/offset work correctly.
    # The default order is by version, which if efficient due to the index.
    # Using the id as a secondary key produces a unique ordering.
    sort = params.pop("sort", None)
    if sort == 'index':
        query = query.order_by(bso.c.sortindex.desc(), bso.c.id.desc())
    elif sort == 'oldest':
        query = query.order_by(bso.c.version.asc(), bso.c.id.asc())
    else:
        query = query.order_by(bso.c.version.desc(), bso.c.id.desc())
    # Apply limit and/or offset.
    limit = params.pop("limit", None)
    if limit is not None:
        query = query.limit(limit)
    offset = params.pop("offset", None)
    if offset is not None:
        query = query.offset(int(offset))
    return query

# Queries operating on a particular item.

DELETE_ITEM = "DELETE FROM %(bso)s WHERE userid=:userid AND "\
              "collection=:collectionid AND id IN "\
              "  (SELECT b.id FROM %(bso)s b, user_collections uc "\
              "   WHERE b.userid=:userid AND uc.userid=:userid "\
              "   AND b.id=:item AND b.collection=uc.collection "\
              "   AND b.ttl>:ttl AND b.version>uc.last_deleted_v)"

ITEM_DETAILS = "SELECT id, sortindex, version, timestamp, payload "\
               "FROM %(bso)s b WHERE collection=:collectionid "\
               "AND userid=:userid AND id=:item AND ttl>:ttl AND version > "\
               "  (SELECT last_deleted_v FROM user_collections uc "\
               "   WHERE uc.userid=:userid AND uc.collection=b.collection)"

ITEM_VERSION = "SELECT version FROM %(bso)s b "\
               "WHERE collection=:collectionid AND userid=:userid "\
               "AND id=:item AND ttl>:ttl AND version > "\
               "  (SELECT last_deleted_v FROM user_collections uc "\
               "   WHERE uc.userid=:userid AND uc.collection=b.collection)"

ALL_COLLECTIONS = "SELECT * FROM user_collections"

ALL_ITEMS = "SELECT * FROM bso"
