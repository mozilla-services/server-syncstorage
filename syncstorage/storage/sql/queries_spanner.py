from sqlalchemy.sql import select, bindparam, text

DATABASE_CREATE_DDL = """\
CREATE TABLE user_collections (
    userid          STRING(MAX) NOT NULL,
    collection      INT64 NOT NULL,
    last_modified   TIMESTAMP NOT NULL
) PRIMARY KEY (userid, collection);

CREATE TABLE bso (
    userid          STRING(MAX) NOT NULL,
    collection      INT64 NOT NULL,
    id              STRING(MAX) NOT NULL,
    sortindex       INT64,
    modified        TIMESTAMP NOT NULL,
    payload         STRING(MAX) NOT NULL,
    ttl             TIMESTAMP NOT NULL
) PRIMARY KEY (userid, collection, id),
  INTERLEAVE IN PARENT user_collections ON DELETE CASCADE;

CREATE INDEX BsoTtl ON bso(ttl);
-- TODO: modified DESC's ideal for the default sort order but client can
-- also specify ASC
CREATE INDEX BsoLastModified ON bso(userid, collection, modified DESC),
  INTERLEAVE IN user_collections;

CREATE TABLE collections (
    collectionid    INT64 NOT NULL,
    name            STRING(MAX) NOT NULL,
) PRIMARY KEY (collectionid);

-- batches is not interleaved w/ user_collections because they can be
-- created before there's a row in that table (test_storage has a test
-- that triggers this). spanner requires the parent's row to be present
-- before the child's
CREATE TABLE batches (
    userid          STRING(MAX) NOT NULL,
    collection      INT64 NOT NULL,
    id              TIMESTAMP NOT NULL,
    bsos            STRING(MAX) NOT NULL,
    expiry          TIMESTAMP NOT NULL
) PRIMARY KEY (userid, collection, id);
"""

COLLECTION_CURRENT_TIMESTAMP = """\
SELECT CURRENT_TIMESTAMP() as now, last_modified FROM user_collections
WHERE userid=@userid AND collection=@collectionid
"""

STORAGE_SIZE = """\
SELECT SUM(payload_size) FROM (
    SELECT CHAR_LENGTH(payload) as payload_size, userid FROM bso
    WHERE userid=@userid AND ttl > CURRENT_TIMESTAMP()
) GROUP BY userid
"""

COLLECTIONS_SIZES = """\
SELECT collection, SUM(payload_size) FROM (
    SELECT CHAR_LENGTH(payload) as payload_size, collection FROM bso
    WHERE userid=@userid AND ttl > CURRENT_TIMESTAMP()
) GROUP BY collection
"""

COLLECTIONS_COUNTS = """
SELECT collection, COUNT(collection) FROM bso WHERE userid=@userid AND
ttl > CURRENT_TIMESTAMP()
GROUP BY collection
"""


def FIND_ITEMS(bso, params):
    """Item search query.

    Unlike all the other pre-built queries, this one really can't be written
    as a simple string.  We need to include/exclude various WHERE clauses
    based on the values provided at runtime.
    """
    fields = params.get("fields", None)
    if fields is None:
        query = select(col for col in bso.columns
                       if col.name != "payload_size")
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
        # Google's suggested forcing both ends of the range here
        # (echoed in Spanner's best practices doc), though it doesn't
        # seem to affect FIND_ITEMS query plan (and why would it?)
        query = query.where(bso.c.modified <= text("CURRENT_TIMESTAMP()"))
    if "newer_eq" in params:
        query = query.where(bso.c.modified >= bindparam("newer_eq"))
        # see above
        query = query.where(bso.c.modified <= text("CURRENT_TIMESTAMP()"))
    if "older" in params:
        query = query.where(bso.c.modified < bindparam("older"))
    if "older_eq" in params:
        query = query.where(bso.c.modified <= bindparam("older_eq"))
    if "ttl" in params:
        query = query.where(bso.c.ttl > bindparam("ttl"))
    sort = params.get("sort", None)
    if sort == 'index':
        order_args = [bso.c.sortindex.desc(), bso.c.id.desc()]
    elif sort == 'oldest':
        order_args = [bso.c.modified.asc(), bso.c.id.asc()]
    else:
        order_args = [bso.c.modified.desc(), bso.c.id.asc()]
    query = query.order_by(*order_args)
    # Apply limit and/or offset.
    limit = params.get("limit", None)
    if limit is not None:
        query = query.limit(bindparam("limit"))
    offset = params.get("offset", None)
    if offset is not None:
        query = query.offset(bindparam("offset"))
    return query
