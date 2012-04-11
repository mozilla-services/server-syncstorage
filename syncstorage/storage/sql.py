# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
SQL backend for syncserver.

This module implements an SQL storage plugin for syncserver.  In the simplest
use case it consists of two database tables:

  collections:  the names and ids of any custom collections
  bso:          the individual BSO items stored in each collection

For efficiency when dealing with large datasets, the plugin also supports
sharding of the BSO items into multiple tables named "bso0" through "bsoN".
This behaviour is off by default; pass shard=True to enable it.

For details of the database schema, see the file "sqlmappers.py".
For details of the prepared queries, see the file "queries.py".

"""

import urlparse
from time import time
import traceback

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, TimeoutError, IntegrityError
from sqlalchemy.sql import (text as sqltext, select, bindparam, insert, update,
                            delete, and_)


from pyramid.threadlocal import get_current_registry

from mozsvc.exceptions import BackendError

from syncstorage.bso import BSO
from syncstorage.util import get_timestamp, from_timestamp
from syncstorage.storage import StorageConflictError
from syncstorage.storage.queries import get_query
from syncstorage.storage.sqlmappers import bso as _bso
from syncstorage.storage.sqlmappers import (tables, collections,
                                            get_bso_table_name, MAX_TTL,
                                            get_bso_table,
                                            get_bso_table_byindex)


_KB = float(1024)

MAX_COLLECTIONS_CACHE_SIZE = 1000

# For efficiency, it's possible to use fixed pre-determined IDs for
# common collection names.  This is the canonical list of such names.
# Non-standard collections will be allocated IDs starting from the
# highest ID in this collection.
STANDARD_COLLECTIONS = {1: 'clients', 2: 'crypto', 3: 'forms', 4: 'history',
                        5: 'keys', 6: 'meta', 7: 'bookmarks', 8: 'prefs',
                        9: 'tabs', 10: 'passwords', 11: 'addons'}

FIRST_CUSTOM_COLLECTION_ID = 100


def _int_now():
    return int(time())


class SQLStorage(object):
    """Storage plugin implemented using an SQL database.

    This class implements the storage plugin API using SQLAlchemy.  You
    must specify the SQLAlchemy database URI string to connect to, and
    can customize behaviour with the following keyword arguments:

        * standard_collections:  use fixed pre-determined ids for common
                                 collection names
        * create_tables:         create the database tables if they don't
                                 exist at startup
        * use_quota/quota_size:  limit per-user storage to a specific quota
        * shard/shardsize:       enable sharding of the BSO table

    """

    def __init__(self, sqluri, standard_collections=False,
                 use_quota=False, quota_size=0, pool_size=100,
                 pool_recycle=60, reset_on_return=True, create_tables=False,
                 shard=False, shardsize=100,
                 pool_max_overflow=10, no_pool=False,
                 pool_timeout=30, **kw):

        self.sqluri = sqluri
        self.driver = urlparse.urlparse(sqluri).scheme

        # Create the SQLAlchemy engine, using the given parameters for
        # connection pooling.  Pooling doesn't work properly for sqlite so
        # it's disabled for that driver regardless of the value of no_pool.
        if no_pool or self.driver == 'sqlite':
            from sqlalchemy.pool import NullPool
            self._engine = create_engine(sqluri, poolclass=NullPool,
                                         logging_name='syncserver')
        else:
            sqlkw = {'pool_size': int(pool_size),
                        'pool_recycle': int(pool_recycle),
                        'logging_name': 'syncserver',
                        'pool_timeout': int(pool_timeout),
                        'max_overflow': int(pool_max_overflow)}

            if self.driver in ('mysql', 'pymsql',
                               'mysql+mysqlconnector'):
                sqlkw['reset_on_return'] = reset_on_return

            self._engine = create_engine(sqluri, **sqlkw)

        # Bind the table metadata to our engine.
        # This is also a good time to create tables if they're missing.
        for table in tables:
            table.metadata.bind = self._engine
            if create_tables:
                table.create(checkfirst=True)
        self.engine_name = self._engine.name
        self.standard_collections = standard_collections
        self.use_quota = use_quota
        self.quota_size = int(quota_size)
        self.shard = shard
        self.shardsize = shardsize
        if self.shard:
            for index in range(shardsize):
                table = get_bso_table_byindex(index)
                table.metadata.bind = self._engine
                if create_tables:
                    table.create(checkfirst=True)
        else:
            _bso.metadata.bind = self._engine
            if create_tables:
                _bso.create(checkfirst=True)
        # There doesn't seem to be a reliable cross-database way to set the
        # initial value of an autoincrement column.  Fake it by inserting
        # a row into the table at the desired start id.
        if self.standard_collections and create_tables:
            zeroth_id = FIRST_CUSTOM_COLLECTION_ID - 1
            query = insert(collections).values(colletionid=zeroth_id, name="")
            self._do_query(query)

        # A cache for the collection_name => collection_id mapping.
        # XXX: need to prevent this growing without bound.
        self._collections_by_name = {}
        self._collections_by_id = {}
        if self.standard_collections:
            for id, name in STANDARD_COLLECTIONS:
                self._collections_by_name[name] = id
                self._collections_by_id[id] = name

        self.logger = get_current_registry()['metlog']

    @classmethod
    def get_name(cls):
        """Return the name of the storage plugin"""
        return 'sql'

    def _safe_execute(self, *args, **kwds):
        """Execute an sqlalchemy query, raise BackendError on failure."""
        try:
            return self._engine.execute(*args, **kwds)
        except (OperationalError, TimeoutError), exc:
            err = traceback.format_exc()
            self.logger.error(err)
            raise BackendError(str(exc))

    def _do_query(self, *args, **kwds):
        """Execute a database query, returning the rowcount."""
        res = self._safe_execute(*args, **kwds)
        try:
            return res.rowcount
        finally:
            res.close()

    def _do_query_fetchone(self, *args, **kwds):
        """Execute a database query, returning the first result."""
        res = self._safe_execute(*args, **kwds)
        try:
            return res.fetchone()
        finally:
            res.close()

    def _do_query_fetchall(self, *args, **kwds):
        """Execute a database query, returning iterator over the results."""
        res = self._safe_execute(*args, **kwds)
        try:
            for row in res:
                yield row
        finally:
            res.close()

    #
    # Users APIs
    #

    def _get_query(self, name, user_id):
        """Get the named pre-built query, sharding by user_id if necessary."""
        if self.shard:
            return get_query(name, user_id)
        return get_query(name)

    def delete_storage(self, user_id):
        """Removes all user data"""
        query = self._get_query('DELETE_USER_BSOS', user_id)
        self._do_query(query, user_id=user_id)
        # XXX see if we want to check the rowcount
        return True

    #
    # Collections APIs
    #

    def _get_collection_id(self, collection_name, create=False):
        """Returns a collection id, given the name.

        If the named collection does not exist then None is returned.  To
        automatically create collections on deman, pass create=True.
        """
        # Grab it from the cache if we can.
        try:
            return self._collections_by_name[collection_name]
        except KeyError:
            pass

        # Try to look it up in the database.
        query = select([collections.c.collectionid])
        query = query.where(collections.c.name == collection_name)
        res = self._do_query_fetchone(query)
        if res is not None:
            collection_id = res[0]
        else:
            # Shall we auto-create it?
            if not create:
                return None
            # Insert it into the database.  This might raise a conflict
            # if it was inserted concurrently by someone else.
            query = insert(collections)
            try:
                self._do_query(query, name=collection_name)
            except IntegrityError:
                # Read the id that was created concurrently.
                collection_id = self._get_collection_id(collection_name)
                if collection_id is None:
                    raise
            else:
                # Read the id that we just created.
                # XXX: cross-database way to get last inserted id?
                collection_id = self._get_collection_id(collection_name)
                assert collection_id is not None

        # Sanity-check that we're not trampling standard collection ids.
        if self.standard_collections:
            assert collection_id >= FIRST_CUSTOM_COLLECTION_ID

        self._cache_collection_data(collection_id, collection_name)
        return collection_id

    def _get_collection_name(self, collection_id):
        try:
            return self._collections_by_id[collection_id]
        except KeyError:
            pass

        query = select([collections.c.name],
                       collections.c.collectionid == collection_id)
        res = self._do_query_fetchone(query)
        if res is None:
            return None

        self._cache_collection_data(collection_id, res[0])
        return res[0]

    def _load_collection_names(self, collection_ids):
        """Load any uncached names for the given collection ids.

        If you have a list of collection ids and you want all their names,
        use this method to prime the internal name cache.  Otherwise you'll
        cause _get_collection_name() to do a separate database query for
        each collection id, which is very inefficient.
        """
        uncached_ids = [id for id in collection_ids
                        if id not in self._collections_by_name]
        if uncached_ids:
            is_uncached = collections.c.collectionid.in_(uncached_ids)
            query = select([collections]).where(is_uncached)
            for res in self._do_query_fetchall(query):
                self._cache_collection_data(res[0], res[1])

    def _cache_collection_data(self, collection_id, collection_name):
        if len(self._collections_by_name) > MAX_COLLECTIONS_CACHE_SIZE:
            msg = "More than %d collections have been created, refusing to cache them all"
            logger.warn(msg % (MAX_COLLECTIONS_CACHE_SIZE,))
        else:
            self._collections_by_name[collection_name] = collection_id
            self._collections_by_id[collection_id] = collection_name

    def get_collection_timestamps(self, user_id):
        """return the collection names for a given user"""
        query = self._get_query('COLLECTIONS_MAX_STAMPS', user_id)
        res = list(self._do_query_fetchall(query, user_id=user_id))
        collection_ids = [collection_id for collection_id, stamp in res]
        self._load_collection_names(collection_ids)
        return dict([(self._get_collection_name(collection_id), stamp)
                     for collection_id, stamp in res])

    def get_storage_timestamp(self, user_id):
        """return the last-modified time for the user's entire storage."""
        stamps = self.get_collection_timestamps(user_id)
        if not stamps:
            return None
        return max(stamps.itervalues())

    def get_collection_counts(self, user_id):
        """Return the collection counts for a given user"""
        ttl = _int_now()
        query = self._get_query('COLLECTIONS_COUNTS', user_id)
        res = list(self._do_query_fetchall(query, user_id=user_id, ttl=ttl))
        collection_ids = [collection_id for collection_id, count in res]
        self._load_collection_names(collection_ids)
        return dict([(self._get_collection_name(collection_id), count)
                      for collection_id, count in res])

    def get_collection_timestamp(self, user_id, collection_name):
        """Returns the last-modified timestamp of a collection."""
        collection_id = self._get_collection_id(collection_name)
        if collection_id is None:
            return None

        query = self._get_query('COLLECTION_TIMESTAMP', user_id)
        res = self._do_query_fetchone(query, user_id=user_id,
                                      collection_id=collection_id)
        return res[0]

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.

        The size is the sum of stored payloads.
        """
        ttl = _int_now()
        query = self._get_query('COLLECTIONS_STORAGE_SIZE', user_id)
        res = list(self._do_query_fetchall(query, user_id=user_id, ttl=ttl))
        collection_ids = [collection_id for collection_id, size in res]
        self._load_collection_names(collection_ids)
        return dict([(self._get_collection_name(col[0]),
                    int(col[1]) / _KB) for col in res])

    #
    # Items APIs
    #
    def item_exists(self, user_id, collection_name, item_id):
        """Returns a timestamp if an item exists in the database."""
        collection_id = self._get_collection_id(collection_name)
        if collection_id is None:
            return None

        query = self._get_query('ITEM_EXISTS', user_id)
        res = self._do_query_fetchone(query, user_id=user_id, item_id=item_id,
                                      collection_id=collection_id)
        if res is None:
            return None
        return res[0]

    def _get_bso_table(self, user_id):
        if self.shard:
            return get_bso_table(user_id, self.shardsize)
        return _bso

    def get_items(self, user_id, collection_name, fields=None, filters=None,
                  limit=None, sort=None):
        """returns items from a collection

        "filter" is a dict used to add conditions to the db query.
        Its keys are the field names on which the condition operates.
        Its values are the values the field should have.
        It can be a single value, or a list. For the latter the in()
        operator is used. For single values, the operator has to be provided.
        """
        bso = self._get_bso_table(user_id)
        collection_id = self._get_collection_id(collection_name)
        if collection_id is None:
            return None

        if fields is None:
            fields = [bso]
        else:
            fields = [getattr(bso.c, field) for field in fields]

        # preparing the where statement
        where = [bso.c.userid == user_id,
                 bso.c.collection == collection_id]

        if filters is not None:
            for field, value in filters.items():
                field = getattr(bso.c, field)

                operator, value = value

                if isinstance(value, (list, tuple)):
                    where.append(field.in_(value))
                else:
                    if operator == '=':
                        where.append(field == value)
                    elif operator == '<':
                        where.append(field < value)
                    elif operator == '>':
                        where.append(field > value)

        if filters is None or 'ttl' not in filters:
            where.append(bso.c.ttl > _int_now())

        where = and_(*where)
        query = select(fields, where)

        if sort is not None:
            if sort == 'oldest':
                query = query.order_by(bso.c.modified.asc())
            elif sort == 'newest':
                query = query.order_by(bso.c.modified.desc())
            else:
                query = query.order_by(bso.c.sortindex.desc())

        if limit is not None and int(limit) > 0:
            query = query.limit(int(limit))

        res = self._do_query_fetchall(query)
        res = [self._row_to_bso(row) for row in res]

        # If the query returned no results, we don't know whether that's
        # because it's empty or because it doesn't exist.
        if self.get_collection_timestamp(user_id, collection_name) is None:
            return None
        return res

    def get_item(self, user_id, collection_name, item_id, fields=None):
        """returns one item"""
        collection_id = self._get_collection_id(collection_name)
        if collection_id is None:
            return None

        bso = self._get_bso_table(user_id)
        if fields is None:
            fields = [bso]
        else:
            fields = [getattr(bso.c, field) for field in fields]
        where = self._get_query('ITEM_ID_COL_USER', user_id)
        query = select(fields, where)

        res = self._do_query_fetchone(query, user_id=user_id,
                                      item_id=item_id,
                                      collection_id=collection_id,
                                      ttl=_int_now())
        if res is None:
            return None
        return self._row_to_bso(res)

    def _row_to_bso(self, row):
        """Convert a database table row into a BSO object."""
        item = dict(row)
        for key in ("userid", "collection", "payload_size", "ttl",):
            item.pop(key, None)
        return BSO(item)

    def _set_item(self, user_id, collection_name, item_id, **values):
        """Adds or update an item"""
        bso = self._get_bso_table(user_id)

        if 'ttl' not in values:
            values['ttl'] = MAX_TTL
        else:
            # ttl is provided in seconds, so we add it
            # to the current timestamp
            values['ttl'] += _int_now()

        last_modified = self.item_exists(user_id, collection_name, item_id)

        if 'payload' in values:
            values['payload_size'] = len(values['payload'])

        collection_id = self._get_collection_id(collection_name, create=True)

        if last_modified is None:   # does not exists
            values['collection'] = collection_id
            values['id'] = item_id
            values['userid'] = user_id
            query = insert(bso).values(**values)
        else:
            if 'id' in values:
                del values['id']
            key = and_(bso.c.id == item_id, bso.c.userid == user_id,
                       bso.c.collection == collection_id)
            query = update(bso).where(key).values(**values)

        try:
            self._do_query(query)
        except IntegrityError:
            raise StorageConflictError()
        return last_modified

    def set_item(self, user_id, collection_name, item_id, storage_time=None,
                 **values):
        """Adds or update an item"""
        if storage_time is None:
            storage_time = get_timestamp()

        if 'payload' in values and 'modified' not in values:
            values['modified'] = storage_time

        return self._set_item(user_id, collection_name, item_id, **values)

    def _get_bso_table_name(self, user_id):
        if self.shard:
            return get_bso_table_name(user_id)
        return 'bso'

    def set_items(self, user_id, collection_name, items, storage_time=None):
        """Adds or update a batch of items.

        Returns a list of success or failures.
        """
        if storage_time is None:
            storage_time = get_timestamp()

        if self.engine_name in ('sqlite', 'postgresql'):
            count = 0
            for item in items:
                if 'id' not in item:
                    continue
                item_id = item['id']
                item['modified'] = storage_time
                self.set_item(user_id, collection_name, item_id, **item)
                count += 1
            return count

        # XXX See if SQLAlchemy knows how to do batch inserts
        # that's quite specific to mysql
        fields = ('id', 'sortindex', 'modified', 'payload',
                  'payload_size', 'ttl')

        table = self._get_bso_table_name(user_id)
        query = 'insert into %s (userid, collection, %s) values ' \
                    % (table, ','.join(fields))

        collection_id = self._get_collection_id(collection_name, create=True)
        values = {}
        values['collection'] = collection_id
        values['user_id'] = user_id

        # building the values batch
        binds = [':%s%%(num)d' % field for field in fields]
        pattern = '(:user_id,:collection,%s) ' % ','.join(binds)

        lines = []
        for num, item in enumerate(items):
            lines.append(pattern % {'num': num})
            for field in fields:
                value = item.get(field)
                if value is None:
                    continue
                if field == 'modified' and value is not None:
                    value = storage_time
                values['%s%d' % (field, num)] = value

            if ('payload%d' % num in values and
                'modified%d' % num not in values):
                values['modified%d' % num] = storage_time

            if values.get('ttl%d' % num) is None:
                values['ttl%d' % num] = 2100000000
            else:
                values['ttl%d' % num] += int(from_timestamp(storage_time))

            if 'payload%d' % num in values:
                size = len(values['payload%d' % num])
                values['payload_size%d' % num] = size

        query += ','.join(lines)

        # allowing updates as well
        query += (' on duplicate key update sortindex = values(sortindex),'
                  'modified = values(modified), payload = values(payload),'
                  'payload_size = values(payload_size),'
                  'ttl = values(ttl)')
        return self._do_query(sqltext(query), **values)

    def delete_item(self, user_id, collection_name, item_id,
                    storage_time=None):
        """Deletes an item"""
        collection_id = self._get_collection_id(collection_name)
        if collection_id is None:
            return False
        query = self._get_query('DELETE_SOME_USER_BSO', user_id)
        rowcount = self._do_query(query, user_id=user_id,
                                  collection_id=collection_id,
                                  item_id=item_id)
        return rowcount == 1

    def delete_items(self, user_id, collection_name, item_ids=None,
                     storage_time=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        collection_id = self._get_collection_id(collection_name)
        if collection_id is None:
            return False
        bso = self._get_bso_table(user_id)
        query = delete(bso)
        where = [bso.c.userid == bindparam('user_id'),
                 bso.c.collection == bindparam('collection_id')]

        if item_ids is not None:
            where.append(bso.c.id.in_(item_ids))

        where = and_(*where)
        query = query.where(where)

        # XXX see if we want to send back more details
        # e.g. by checking the rowcount
        rowcount = self._do_query(query, user_id=user_id,
                                  collection_id=collection_id)
        return rowcount > 0

    def get_total_size(self, user_id):
        """Returns the total size in KB of a user storage.

        The size is the sum of stored payloads.
        """
        query = self._get_query('USER_STORAGE_SIZE', user_id)
        res = self._do_query_fetchone(query, user_id=user_id,
                                      ttl=_int_now())
        if res is None or res[0] is None:
            return 0.0
        return int(res[0]) / _KB

    def get_size_left(self, user_id):
        """Returns the storage left for a user"""
        return self.quota_size - self.get_total_size(user_id)
