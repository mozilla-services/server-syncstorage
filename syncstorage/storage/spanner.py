# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Spanner backend wrapper for syncstorage.

This module implements a Spanner layer for the SyncStorage backend API.

"""

import contextlib
import datetime
import functools
import json
import logging
import math
import threading

from google.api_core.exceptions import Aborted, AlreadyExists
from google.cloud import spanner
from google.cloud.spanner_v1.pool import SessionCheckout
from google.cloud.spanner_v1 import param_types
from mozsvc.metrics import metrics_timer

from syncstorage.bso import BSO
from syncstorage.util import get_timestamp
from syncstorage.storage import (SyncStorage,
                                 ConflictError,
                                 CollectionNotFoundError,
                                 ItemNotFoundError,
                                 InvalidBatch,
                                 InvalidOffsetError,
                                 BATCH_LIFETIME)
from syncstorage.storage.sql import (
    FIRST_CUSTOM_COLLECTION_ID,
    MAX_COLLECTIONS_CACHE_SIZE,
    STANDARD_COLLECTIONS,
    queries_generic as queries,
    ts2bigint,
    bigint2ts)
from syncstorage.storage.sql.dbconnect import MAX_TTL, bso
from syncstorage.storage.sql.queries_spanner import (
    COLLECTION_CURRENT_TIMESTAMP,
    COLLECTIONS_SIZES,
    COLLECTIONS_COUNTS,
    FIND_ITEMS,
    STORAGE_SIZE,
)

logger = logging.getLogger(__name__)

INT64_MAX = 2 ** 63 - 1

EPOCH = datetime.datetime.utcfromtimestamp(0)

# a bogus FIND_ITEMS bind param
BOGUS_ID = object()


def dt2ts(dt):
    """Convert a Python datetime to seconds"""
    val = (dt.replace(tzinfo=None) - EPOCH).total_seconds()
    return get_timestamp(math.floor(val * 100) / 100)


def ts2dt(ts):
    """Convert a timestamp to Python datetime"""
    return datetime.datetime.utcfromtimestamp(ts)


def with_session(func):
    @functools.wraps(func)
    def with_session_wrapper(self, *args, **kwds):
        if hasattr(self._tldata, "session"):
            return func(self, self._tldata.session, *args, **kwds)
        with SpannerStorageSession(self) as spanner_session:
            with SessionCheckout(self._pool) as session:
                with session.transaction() as transaction:
                    spanner_session.set_transaction(transaction)
                    return func(self, self._tldata.session, *args, **kwds)

    return with_session_wrapper


def user_key(user):
    """Create a compound user-key using the fxa_uid and fxa_kid

    Joined here rather than used as a compound primary key to allow re-use
    of existing queries.

    """
    return ":".join([user["fxa_uid"], user["fxa_kid"]])


def getq(query):
    kwargs = {}
    if "%(bso)s" in query:
        kwargs["bso"] = "bso"
    if "%(bui)s" in query:
        raise RuntimeError("Unexpected batch query (bui)")
    if "%(ids)s" in query:
        kwargs["ids"] = "@ids"
    if kwargs:
        query = query % kwargs
    return query.replace(":", "@")


class SpannerStorage(SyncStorage):
    """Storage plugin implemented using a Spanner database.

    This class implements the storage plugin API using Google Spanner.  You
    must specify the Spanner database URI string to connect to, as follows:

        spanner://INSTANCE_ID:DATABASE_ID

    """

    def __init__(self, sqluri, standard_collections=False, **dbkwds):
        self.sqluri = sqluri
        instance_id, database_id = sqluri[len("spanner://"):].split(":")
        self.client = spanner.Client()
        self._instance = self.client.instance(instance_id)
        self._pool = pool = spanner.BurstyPool(
            target_size=int(dbkwds.get("pool_size", 100))
        )
        self._database = self._instance.database(database_id, pool=pool)

        # for debugging: support disabling FORCE_INDEX=BsoLastModified
        self._force_bsolm_index = dbkwds.get("_force_bsolm_index", True)
        # split FIND_ITEMS' query in 2 (first query BsoLastModified
        # then query bso with its results)
        self._bsolm_index_separate = dbkwds.get(
            "_bsolm_index_separate",
            False
        )
        if self._bsolm_index_separate:
            assert self._force_bsolm_index, \
                "_bsolm_index_separate requires _force_bsolm_index"

        self.standard_collections = standard_collections
        if self.standard_collections and dbkwds.get("create_tables", False):
            raise Exception("Creating tables in Spanner must be done manually")

        # A local in-memory cache for the name => collectionid mapping.
        self._collections_by_name = {}
        self._collections_by_id = {}
        if self.standard_collections:
            for id, name in STANDARD_COLLECTIONS.iteritems():
                self._collections_by_name[name] = id
                self._collections_by_id[id] = name

        # A thread-local to track active sessions.
        self._tldata = threading.local()

    def _lock_write_precondition(self, session, user, collection):
        """'Lock' the precondition on the collection

        Get the timestamp of the collection in use, utilizing it as
        precondition for transaction invalidation.

        """
        result = session.transaction.execute_sql(
            COLLECTION_CURRENT_TIMESTAMP,
            params={"userid": user_key(user),
                    "collectionid": collection},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64}
        ).one_or_none()
        if result:
            current_ts, last_modified = result
            session.collection_ts = dt2ts(last_modified)
            session.timestamp_precise = current_ts
            session.timestamp = dt2ts(current_ts)
            # Forbid the write if it would not properly incr the timestamp.
            if session.collection_ts >= session.timestamp:
                msg = "last_modified: %s (%s) current_ts: %s (%s)" % (
                    last_modified,
                    session.collection_ts,
                    current_ts,
                    dt2ts(current_ts)
                )
                raise ConflictError("lock_for_write %s" % msg)
        else:
            result = session.transaction.execute_sql(
                "SELECT CURRENT_TIMESTAMP()"
            ).one()[0]
            session.timestamp_precise = result
            session.timestamp = dt2ts(result)
            session.collection_ts = 0

    @contextlib.contextmanager
    def lock_for_read(self, ruser, collection):
        with SpannerStorageSession(self) as spanner_session:
            with self._database.snapshot(multi_use=True) as txn:
                spanner_session.set_transaction(txn)
                yield None

    @contextlib.contextmanager
    def lock_for_write(self, user, collection):
        with SpannerStorageSession(self) as spanner_session:
            collectionid = self._get_collection_id(collection, True)
            with SessionCheckout(self._pool) as session:
                with session.transaction() as transaction:
                    spanner_session.set_transaction(transaction)
                    self._lock_write_precondition(
                        spanner_session,
                        user,
                        collectionid
                    )
                    try:
                        yield None
                    except Aborted:
                        raise ConflictError

    #
    # APIs to operate on the entire storage.
    #

    @with_session
    def get_storage_timestamp(self, session, user):
        """Returns the storage timestamp for the user"""
        result = session.transaction.execute_sql(
            getq(queries.STORAGE_TIMESTAMP),
            params={"userid": user_key(user)},
            param_types={"userid": param_types.STRING}
        ).one()[0]
        return dt2ts(result) if result else 0

    @with_session
    def get_collection_timestamps(self, session, user):
        """Returns the collection timestamps for a user."""
        userid = user_key(user)
        res = session.transaction.execute_sql(
            getq(queries.COLLECTIONS_TIMESTAMPS),
            params={"userid": userid},
            param_types={"userid": param_types.STRING}
        )
        res = self._map_collection_names(res)
        for collection in res:
            res[collection] = dt2ts(res[collection])
        return res

    @with_session
    def get_collection_counts(self, session, user):
        """Returns the collection counts."""
        userid = user_key(user)
        res = session.transaction.execute_sql(
            COLLECTIONS_COUNTS,
            params={"userid": userid},
            param_types={"userid": param_types.STRING}
        )
        return self._map_collection_names(res)

    @with_session
    def get_collection_sizes(self, session, user):
        """Returns the total size for each collection."""
        userid = user_key(user)
        res = session.transaction.execute_sql(
            COLLECTIONS_SIZES,
            params={"userid": userid},
            param_types={"userid": param_types.STRING}
        )
        rows = ((row[0], int(row[1])) for row in res)
        return self._map_collection_names(rows)

    @with_session
    def get_total_size(self, session, user, recalculate=False):
        """Returns the total size a user's stored data."""
        userid = user_key(user)
        size = session.transaction.execute_sql(
            STORAGE_SIZE,
            params={"userid": userid},
            param_types={"userid": param_types.STRING}
        ).one_or_none()
        return int(size[0] if size else 0)

    @with_session
    def delete_storage(self, session, user):
        """Removes all data for the user."""
        userid = user_key(user)
        session.transaction.execute_update(
            getq(queries.DELETE_ALL_COLLECTIONS),
            params={"userid": userid},
            param_types={"userid": param_types.STRING}
        )

    #
    # APIs to operate on an individual collection
    #

    @with_session
    def get_collection_timestamp(self, session, user, collection):
        """Returns the last-modified timestamp of a collection."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        # The last-modified timestamp may be cached on the session.
        if session.collection_ts:
            return session.collection_ts

        # Otherwise we need to look it up in the database.
        result = session.transaction.execute_sql(
            getq(queries.COLLECTION_TIMESTAMP),
            params={"userid": userid,
                    "collectionid": collectionid},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64}
        ).one_or_none()
        if result is None:
            raise CollectionNotFoundError
        return dt2ts(result[0])

    @with_session
    def get_items(self, session, user, collection, **params):
        """Returns items from a collection."""
        return self._find_items(session, user, collection, **params)

    @with_session
    def get_item_ids(self, session, user, collection, **params):
        """Returns item ids from a collection."""
        # Select only the fields we need, including those that might
        # be used for limit/offset pagination.
        params["fields"] = ["id", "modified", "sortindex"]
        res = self._find_items(session, user, collection, **params)
        res["items"] = [item["id"] for item in res["items"]]
        return res

    def _find_items(self, session, user, collection, **params):
        """Find items matching the given search parameters."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        bind = {"userid": userid,
                "collectionid": collectionid}
        bind_types = {"userid": param_types.STRING,
                      "collectionid": param_types.INT64}
        ts = session.timestamp or get_timestamp()
        if "ttl" not in params:
            params["ttl"] = ts

        if "ids" in params:
            for i, id in enumerate(params["ids"], 1):
                bind["id_%d" % (i,)] = id
                bind_types["id_%d" % (i,)] = param_types.STRING

        if "sort" in params:
            bind["sort"] = params["sort"]
            bind_types["sort"] = param_types.STRING

        # We always fetch one more item than necessary, so we can tell whether
        # there are additional items to be fetched with next_offset.
        limit = params.get("limit")
        if limit is not None:
            params["limit"] = bind["limit"] = limit + 1
            bind_types["limit"] = param_types.INT64
        offset = params.pop("offset", None)
        if offset is not None:
            self.decode_offset(params, offset)
            bind["offset"] = params["offset"]
            bind_types["offset"] = param_types.INT64
            if limit is None:
                # avoid sqlalchemy defaulting to LIMIT -1 when none provided.
                # subtract offset to avoid overflow errors (that only occur w/
                # a FORCE_INDEX= directive) OutOfRange: 400 int64 overflow:
                # <INT64_MAX> + offset
                params["limit"] = bind["limit"] = INT64_MAX - params["offset"]
                bind_types["limit"] = param_types.INT64

        # Convert timestamp types
        for ts_var in ["newer", "newer_eq", "older", "older_eq", "ttl"]:
            if ts_var in params:
                bind[ts_var] = ts2dt(params[ts_var])
                bind_types[ts_var] = param_types.TIMESTAMP

        # Generate the query
        fields = params.get("fields")
        offset_params = params

        if self._bsolm_index_separate and fields is None:
            # Split the get_items query (TODO: get_item_ids could
            # reference solely the index) query in 2:
            # #1: query BsoLastModified directly for bso ids
            # #2: query bso w/ 'id IN UNNEST([<ids from #1>])'

            bsolm_params = params.copy()
            bsolm_params["fields"] = ["id"]
            bsolm_query = FIND_ITEMS(bso, bsolm_params)
            bsolm_query = str(bsolm_query.compile())
            bsolm_query = bsolm_query.replace(
                "FROM bso",
                "FROM bso@{FORCE_INDEX=BsoLastModified}"
            )
            bsolm_query = bsolm_query.replace(":", "@")

            result = session.transaction.execute_sql(
                bsolm_query,
                params=bind,
                param_types=bind_types,
            )
            ids = [row[0] for row in list(result)]

            # keep the orig. for encode_next_offset
            offset_params = params.copy()

            # Setup a 'id IN (:id_1)' bind param for #2 (replacing it
            # below)
            params["ids"] = [BOGUS_ID]
            bind["ids"] = ids
            bind_types["ids"] = param_types.Array(param_types.STRING)

            # offset/limit are accomplished in query #1 and don't make
            # sense for #2
            if limit is not None:
                del params["limit"]
                del bind["limit"]
                del bind_types["limit"]
            if offset is not None:
                # restore this later
                del params["offset"]
                del bind["offset"]
                del bind_types["offset"]
            # simiarly modified ranges/ttl aren't needed in #2
            for param in ("newer", "newer_eq", "older", "older_eq", "ttl"):
                params.pop(param, None)

        query = FIND_ITEMS(bso, params)
        query = str(query.compile())
        if self._force_bsolm_index and not self._bsolm_index_separate:
            query = query.replace(
                "FROM bso",
                "FROM bso@{FORCE_INDEX=BsoLastModified}"
            )
        if self._bsolm_index_separate and fields is None:
            query = query.replace(
                "bso.id IN (:id_1)",
                "bso.id IN UNNEST(@ids)"
            )
        query = query.replace(":", "@")

        result = session.transaction.execute_sql(
            query,
            params=bind,
            param_types=bind_types,
        )
        rows = list(result)
        items = []

        if rows:
            # Get the names of the columns for the result row placements
            fields = [x.name for x in result.fields]
            items = [self._row_to_bso(zip(fields, row), int(ts))
                     for row in rows]

        # If the query returned no results, we don't know whether that's
        # because it's empty or because it doesn't exist.  Read the collection
        # timestamp and let it raise CollectionNotFoundError if necessary.
        if not items:
            self.get_collection_timestamp(user, collection)
        # Check if we read past the original limit, set next_offset if so.
        next_offset = None
        if limit is not None and len(items) > limit:
            items = items[:-1]
            next_offset = self.encode_next_offset(offset_params, items)
        return {
            "items": items,
            "next_offset": next_offset,
        }

    def encode_next_offset(self, params, items):
        sort = params.get("sort", None)
        # Use a simple numeric offset for sortindex ordering.
        if sort == "index":
            return str(params.get("offset", 0) + len(items))
        # Find an appropriate upper bound for faster timestamp ordering.
        bound = items[-1]["modified"]
        bound_as_bigint = ts2bigint(bound)
        # Count how many previous items have that same timestamp, and hence
        # will need to be skipped over.  The number of matches here is limited
        # by upload batch size.
        offset = 1
        i = len(items) - 2
        while i >= 0 and items[i]["modified"] == bound:
            offset += 1
            i -= 1
        # If all items in this batch have the same timestamp, we may also need
        # to skip some items from the previous batch.  This should only occur
        # with a very small "limit" parameter, e.g. during testing.
        if i < 0:
            if sort == "oldest":
                prev_bound = params.get("newer_eq", None)
            else:
                prev_bound = params.get("older_eq", None)
            if prev_bound and ts2bigint(prev_bound) == bound_as_bigint:
                offset += params["offset"]
        # Encode them as a simple pair of integers.
        return "%d:%d" % (bound_as_bigint, offset)

    def decode_offset(self, params, offset):
        sort = params.get("sort", None)
        try:
            if sort == "index":
                # When sorting by sortindex, it's just a numeric offset.
                params["offset"] = int(offset)
            else:
                # When sorting by timestamp, it's a (bound, offset) pair.
                bound, offset = map(int, offset.split(":", 1))
                bound = bigint2ts(bound)
                # Queries with "newer" should always produce bound > newer
                if bound < params.get("newer", bound):
                    raise InvalidOffsetError(offset)
                # Queries with "older" should always produce bound < older
                if bound > params.get("older", bound):
                    raise InvalidOffsetError(offset)
                # The bound determines the starting point of the sort order.
                params["offset"] = offset
                if sort == "oldest":
                    params["newer_eq"] = bound
                else:
                    params["older_eq"] = bound
        except ValueError:
            raise InvalidOffsetError(offset)

    def _row_to_bso(self, row, timestamp):
        """Convert a database table row into a BSO object."""
        item = dict(row)
        for key in ("userid", "collection"):
            item.pop(key, None)
        ts = item.get("modified")
        if ts is not None:
            item["modified"] = dt2ts(ts)
        # Convert the ttl back into an offset from the current time.
        ttl = item.get("ttl")
        if ttl is not None:
            item["ttl"] = int(dt2ts(ttl) - timestamp)
        return BSO(item)

    @with_session
    def set_items(self, session, user, collection, items):
        """Creates or updates multiple items in a collection."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection, create=True)
        self._touch_collection(session, userid, collectionid)
        if not items:
            return session.timestamp

        item_ids = [x["id"] for x in items]
        bind_names = ["@id_%d" % x for x in range(1, len(items) + 1)]
        bind = {"userid": userid,
                "collectionid": collectionid}
        bind_types = {"userid": param_types.STRING,
                      "collectionid": param_types.INT64}
        for i, id in enumerate(item_ids, 1):
            bind["id_%d" % (i,)] = id
            bind_types["id_%d" % (i,)] = param_types.STRING
        q = """\
        SELECT id FROM bso
        WHERE userid=@userid AND collection=@collectionid AND id in (%s)"""
        q = q % ', '.join(bind_names)
        result = session.transaction.execute_sql(
            q,
            params=bind,
            param_types=bind_types
        )
        existing = {row[0] for row in result}

        insert_rows = []
        updates = []
        for item in items:
            if item["id"] in existing:
                updates.append(item)
                continue
            row = self._prepare_bso_row(
                session,
                userid,
                collectionid,
                item["id"],
                item
            )
            insert_rows.append(row)

        if insert_rows:
            session.transaction.insert(
                "bso",
                columns=["userid", "collection", "id", "sortindex", "modified",
                         "payload", "ttl"],
                values=insert_rows)

        for item in updates:
            cols, vals = self._prepare_update_bso_row(
                session,
                userid,
                collectionid,
                item["id"],
                item
            )
            session.transaction.update(
                "bso",
                columns=cols,
                values=[vals]
            )
        return session.timestamp

    @with_session
    def create_batch(self, session, user, collection):
        """Creates a batch in batch_uploads table"""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        # Use a more precise timestamp for the batch id (timestamp in
        # milliseconds)
        id = ts2bigint((session.timestamp_precise.replace(tzinfo=None)
                        - EPOCH).total_seconds())
        try:
            session.transaction.execute_update(
                """\
                INSERT INTO batches (userid, collection, id, bsos, expiry)
                VALUES (@userid, @collectionid, @id, @bsos, @expiry)
                """,
                params={"userid": userid,
                        "collectionid": collectionid,
                        "id": ts2dt(id / 1000.0),
                        "bsos": "",
                        "expiry": ts2dt(session.timestamp) +
                        datetime.timedelta(seconds=BATCH_LIFETIME)},
                param_types={"userid": param_types.STRING,
                             "collectionid": param_types.INT64,
                             "id": param_types.TIMESTAMP,
                             "bsos": param_types.STRING,
                             "expiry": param_types.TIMESTAMP}
            )
        except AlreadyExists:
            # The user tried to create two batches with the same timestamp.
            raise ConflictError
        return id

    @with_session
    def valid_batch(self, session, user, collection, batchid):
        """Checks to see if the batch ID is valid and still open"""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        batchid = ts2dt(batchid / 1000.0)
        q = """\
        SELECT id FROM batches
        WHERE userid=@userid AND collection=@collectionid AND id=@id"""
        result = session.transaction.execute_sql(
            q,
            params={"userid": userid,
                    "collectionid": collectionid,
                    "id": batchid},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "id": param_types.TIMESTAMP}
        ).one_or_none()
        return bool(result)

    @metrics_timer("syncstorage.storage.sql.append_items_to_batch")
    @with_session
    def append_items_to_batch(self, session, user, collection, batchid,
                              items):
        """Inserts items into batch_upload_items"""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        batchid = ts2dt(batchid / 1000.0)
        items_json = [json.dumps(item) for item in items]
        bsos = "%s%s" % ("\n".join(items_json), "\n" if items_json else "")
        result = session.transaction.execute_update(
            """\
            UPDATE batches SET bsos = CONCAT(bsos, @bsos)
            WHERE userid=@userid AND collection=@collectionid AND id=@id AND
            expiry > CURRENT_TIMESTAMP()
            """,
            params={"userid": userid,
                    "collectionid": collectionid,
                    "id": batchid,
                    "bsos": bsos},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "id": param_types.TIMESTAMP}
        )
        if result != 1:
            raise InvalidBatch

    @metrics_timer("syncstorage.storage.sql.apply_batch")
    @with_session
    def apply_batch(self, session, user, collection, batchid):
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        batchid = ts2dt(batchid / 1000.0)
        q = "SELECT bsos FROM batches WHERE userid=@userid AND " \
            "collection=@collectionid AND id=@id"
        result_set = session.transaction.execute_sql(
            q,
            params={"userid": userid,
                    "collectionid": collectionid,
                    "id": batchid},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "id": param_types.TIMESTAMP}
        )

        result = result_set.one_or_none()
        if not result:
            self._touch_collection(session, userid, collectionid)
            return session.timestamp
        bsos = [BSO(json.loads(x)) for x in result[0].split("\n") if x]
        session.transaction.execute_sql(
            "DELETE FROM batches "
            "WHERE userid=@userid AND collection=@collectionid "
            "AND id=@id",
            params={"userid": userid,
                    "collectionid": collectionid,
                    "id": batchid},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "id": param_types.TIMESTAMP}
        )
        return self.set_items(user, collection, bsos)

    @metrics_timer("syncstorage.storage.sql.close_batch")
    @with_session
    def close_batch(self, session, user, collection, batchid):
        """Apply batch now closes the batch so this is a no-op"""
        pass

    @with_session
    def delete_collection(self, session, user, collection):
        """Deletes an entire collection."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        count = session.transaction.execute_update(
            getq(queries.DELETE_COLLECTION_ITEMS),
            params={"userid": userid,
                    "collectionid": collectionid},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64}
        )
        count += session.transaction.execute_update(
            getq(queries.DELETE_COLLECTION),
            params={"userid": userid,
                    "collectionid": collectionid},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64}
        )
        if count == 0:
            raise CollectionNotFoundError
        return self.get_storage_timestamp(user)

    @with_session
    def delete_items(self, session, user, collection, items):
        """Deletes multiple items from a collection."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        self._touch_collection(session, userid, collectionid)
        bind_names = ["@id_%d" % x for x in range(1, len(items) + 1)]
        bind = {"userid": userid,
                "collectionid": collectionid}
        bind_types = {"userid": param_types.STRING,
                      "collectionid": param_types.INT64}
        for i, id in enumerate(items, 1):
            bind["id_%d" % (i,)] = id
            bind_types["id_%d" % (i,)] = param_types.STRING
        session.transaction.execute_update(
            getq(
                queries.DELETE_ITEMS).replace(
                "@ids",
                '(%s)' % ', '.join(bind_names)
            ),
            params=bind,
            param_types=bind_types
        )
        return session.timestamp

    def _touch_collection(self, session, userid, collectionid):
        session.transaction.insert_or_update(
            "user_collections",
            columns=["userid", "collection", "last_modified"],
            values=[[userid, collectionid, ts2dt(session.timestamp)]]
        )

    #
    # Items APIs
    #

    @with_session
    def get_item_timestamp(self, session, user, collection, item):
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        result = session.transaction.execute_sql(
            getq(queries.ITEM_TIMESTAMP),
            params={"userid": userid,
                    "collectionid": collectionid,
                    "item": item,
                    "ttl": ts2dt(session.timestamp or get_timestamp())},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "item": param_types.STRING,
                         "ttl": param_types.TIMESTAMP}
        ).one_or_none()
        if not result:
            raise ItemNotFoundError
        return dt2ts(result[0])

    @with_session
    def get_item(self, session, user, collection, item):
        """Returns one item from a collection."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        row = session.transaction.execute_sql(
            getq(queries.ITEM_DETAILS),
            params={"userid": userid,
                    "collectionid": collectionid,
                    "item": item,
                    "ttl": ts2dt(session.timestamp or get_timestamp())},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "item": param_types.STRING,
                         "ttl": param_types.TIMESTAMP}
        ).one_or_none()
        if row is None:
            raise ItemNotFoundError

        # Mix-in the column names as row_to_bso needs
        row = zip(["id", "sortindex", "modified", "payload"], row)

        result = session.transaction.execute_sql(
            "SELECT CURRENT_TIMESTAMP()").one()
        current_ts = dt2ts(result[0])
        return self._row_to_bso(row, int(current_ts))

    @with_session
    def set_item(self, session, user, collection, item, data):
        data["id"] = item
        self.set_items(user, collection, [data])
        return {
            "modified": session.timestamp
        }

    def _prepare_bso_row(self, session, userid, collectionid, item, data):
        """Prepare row data for storing the given BSO."""
        row = [userid, collectionid, item]
        if "sortindex" in data:
            row.append(data["sortindex"])
        else:
            row.append(None)
        row.append(ts2dt(session.timestamp))
        # If a payload is provided, make sure to update dependent fields.
        row.append(data["payload"] if "payload" in data else "")

        # If provided, ttl will be an offset in seconds.
        # Add it to the current timestamp to get an absolute time.
        # If not provided or None, this means no ttl should be set.
        if "ttl" in data:
            if data["ttl"] is None:
                row.append(ts2dt(MAX_TTL))
            else:
                ttl = session.timestamp + data["ttl"]
                row.append(ts2dt(ttl))
        else:
            # XXX: should probably default it on the column
            row.append(ts2dt(MAX_TTL))
        return row

    def _prepare_update_bso_row(self, session, userid, collectionid, id, data):
        """Prepare row data for storing the given BSO."""
        cols = ["userid", "collection", "id"]
        row = [userid, collectionid, id]
        if "sortindex" in data:
            cols.append("sortindex")
            row.append(data["sortindex"])

        # If a payload is provided, make sure to update dependent fields.
        if "payload" in data:
            cols.append("modified")
            row.append(ts2dt(session.timestamp))
            cols.append("payload")
            row.append(data["payload"])

        # If provided, ttl will be an offset in seconds.
        # Add it to the current timestamp to get an absolute time.
        # If not provided or None, this means no ttl should be set.
        if "ttl" in data:
            cols.append("ttl")
            if data["ttl"] is None:
                row.append(ts2dt(MAX_TTL))
            else:
                ttl = session.timestamp + data["ttl"]
                row.append(ts2dt(ttl))
        return cols, row

    @with_session
    def delete_item(self, session, user, collection, item):
        """Deletes a single item from a collection."""
        userid = user_key(user)
        collectionid = self._get_collection_id(collection)
        self._touch_collection(session, userid, collectionid)
        rowcount = session.transaction.execute_update(
            getq(queries.DELETE_ITEM),
            params={"userid": userid,
                    "collectionid": collectionid,
                    "item": item,
                    "ttl": ts2dt(session.timestamp)},
            param_types={"userid": param_types.STRING,
                         "collectionid": param_types.INT64,
                         "item": param_types.STRING,
                         "ttl": param_types.TIMESTAMP}
        )
        if not rowcount:
            raise ItemNotFoundError
        return session.timestamp

    #
    # Private methods for manipulating collections.
    #

    def _get_collection_id(self, collection, create=False):
        """Returns a collection id, given the name.

        If the named collection does not exist then CollectionNotFoundError
        will be raised.  To automatically create collections on demand, pass
        create=True.

        These queries run outside any transactions.

        """
        # Grab it from the cache if we can.
        try:
            return self._collections_by_name[collection]
        except KeyError:
            pass

        # Try to look it up in the database.
        with self._database.snapshot() as snapshot:
            result = snapshot.execute_sql(
                getq(queries.COLLECTION_ID),
                params={"name": collection},
                param_types={"name": param_types.STRING}
            ).one_or_none()

        if not result:
            # Shall we auto-create it?
            if not create:
                raise CollectionNotFoundError

            # Insert it into the database. We don't have autoincrement
            # primary keys, so we must read first to see if we can try
            # and insert a higher ordered one
            def create_collection(transaction):
                result = transaction.execute_sql(
                    "SELECT MAX(collectionid) from collections"
                ).one()
                collectionid = result[0] + 1 if result[0] else 1
                # Choose a collection ID at least as large as the first
                # custom id
                if self.standard_collections:
                    collectionid = max(collectionid,
                                       FIRST_CUSTOM_COLLECTION_ID)
                transaction.insert(
                    "collections",
                    columns=["collectionid", "name"],
                    values=[[collectionid, collection]]
                )
                return collectionid

            try:
                collectionid = self._database.run_in_transaction(
                    create_collection)
            except AlreadyExists as e:
                # This might raise a conflict if it was inserted
                # concurrently by someone else.
                raise ConflictError("getcid %s" % e)
        else:
            collectionid = result[0]

        self._cache_collection_id(collectionid, collection)
        return collectionid

    def _load_collection_names(self, collection_ids):
        """Load any uncached names for the given collection ids.

        If you have a list of collection ids and you want all their names,
        use this method to prime the internal name cache.  Otherwise you"ll
        cause _get_collection_name() to do a separate database query for
        each collection id, which is very inefficient.

        Since it may not be possible to cache the names for all the requested
        collections, this method also returns a mapping from ids to names.
        """
        names = {}
        uncached_ids = []
        # Extract as many names as possible from the cache, and
        # build a list of any ids whose names are not cached.
        for id in collection_ids:
            try:
                names[id] = self._collections_by_id[id]
            except KeyError:
                uncached_ids.append(id)
        # Use a single query to fetch the names for all uncached collections.
        if uncached_ids:
            bind_names = ["@id_%d" % x
                          for x in range(1, len(uncached_ids) + 1)]
            bind = {}
            bind_types = {}
            for i, id in enumerate(uncached_ids, 1):
                bind["id_%d" % (i,)] = id
                bind_types["id_%d" % (i,)] = param_types.INT64

            with self._database.snapshot() as snapshot:
                uncached_names = snapshot.execute_sql(
                    getq(queries.COLLECTION_NAMES).replace(
                        "@ids",
                        '(%s)' % ', '.join(bind_names)
                    ),
                    params=bind,
                    param_types=bind_types
                )
            for id, name in uncached_names:
                names[id] = name
                self._cache_collection_id(id, name)
        # Check that we actually got a name for each specified id.
        for id in collection_ids:
            if id not in names:
                msg = "Collection id %d has no corresponding name."
                msg += "  Possible database corruption?"
                raise KeyError(msg % (id,))
        return names

    def _map_collection_names(self, values):
        """Helper to create a map of collection names to values.

        Given a sequence of (collectionid, value) pairs, this method will
        return a mapping from collection names to their corresponding value.
        """
        values = list(values)
        collection_ids = [collectionid for collectionid, value in values]
        names = self._load_collection_names(collection_ids)
        return dict([(names[id], value) for id, value in values])

    def _cache_collection_id(self, collectionid, collection):
        """Cache the given collection (id, name) pair for fast lookup."""
        if len(self._collections_by_name) > MAX_COLLECTIONS_CACHE_SIZE:
            msg = "More than %d collections have been created, " \
                  "refusing to cache them all"
            logger.warn(msg % (MAX_COLLECTIONS_CACHE_SIZE,))
        else:
            self._collections_by_name[collection] = collectionid
            self._collections_by_id[collectionid] = collection


class SpannerStorageSession(object):
    """Object representing a data access session."""

    def __init__(self, storage):
        self.storage = storage
        self.txn = None
        self.collection_ts = None
        self.timestamp = None

    def __enter__(self):
        self.storage._tldata.session = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.storage._tldata.session

    @property
    def transaction(self):
        return self.txn

    def set_transaction(self, txn):
        """Sets a transaction object on the storage session

        This will effectively reset the storage session object back to
        its default values.

        """
        self.collection_ts = None
        self.txn = txn
