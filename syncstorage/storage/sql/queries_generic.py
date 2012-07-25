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

INSERT_COLLECTION = "INSERT INTO collections (collectionid, name) "\
                    "VALUES (:collectionid, :name)"

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
    # Filter by the various query parameters.
    if "items" in params:
        # Sadly, we can't use a bindparam in an "IN" expression.
        query = query.where(bso.c.id.in_(params.pop("items")))
    if "older" in params:
        query = query.where(bso.c.modified < bindparam("older"))
    if "newer" in params:
        query = query.where(bso.c.modified > bindparam("newer"))
    if "ttl" in params:
        query = query.where(bso.c.ttl > bindparam("ttl"))
    # Sort it in the order requested.
    # We always sort by *something*, so that limit/offset work correctly.
    # The default order is by modified, which if efficient due to the index.
    # Using the id as a secondary key produces a unique ordering.
    sort = params.pop("sort", None)
    if sort == 'index':
        query = query.order_by(bso.c.sortindex.desc(), bso.c.id.desc())
    elif sort == 'oldest':
        query = query.order_by(bso.c.modified.asc(), bso.c.id.asc())
    else:
        query = query.order_by(bso.c.modified.desc(), bso.c.id.desc())
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
              "collection=:collectionid AND id=:item AND ttl>:ttl"\

ITEM_DETAILS = "SELECT id, sortindex, modified, payload FROM %(bso)s "\
               "WHERE collection=:collectionid AND userid=:userid "\
               "AND id=:item AND ttl>:ttl"

ITEM_TIMESTAMP = "SELECT modified FROM %(bso)s "\
                 "WHERE collection=:collectionid AND userid=:userid "\
                 "AND id=:item AND ttl>:ttl"
