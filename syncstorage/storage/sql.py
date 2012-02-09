# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
SQL backend for syncserver.

This module implements an SQL storage plugin for syncserver.  In the simplest
use case it consists of two database tables:

  collections:  the names of per-user custom collections
  bso:          the individual BSO items stored in each collection

For efficiency when dealing with large datasets, the plugin also supports
sharding of the BSO items into multiple tables named "bso0" through "bsoN".
This behaviour is off by default; pass shard=True to enable it.

For details of the database schema, see the file "sqlmappers.py".
For details of the prepared queries, see the file "queries.py".

"""

import urlparse
from time import time
from collections import defaultdict
import traceback

from sqlalchemy import create_engine
from sqlalchemy.sql import (text as sqltext, select, bindparam, insert, update,
                            and_)
from sqlalchemy.sql.expression import _generative, Delete, _clone, ClauseList
from sqlalchemy import util
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.exc import OperationalError, TimeoutError


from mozsvc.exceptions import BackendError

from syncstorage import logger
from syncstorage.bso import BSO
from syncstorage.util import get_timestamp, from_timestamp
from syncstorage.storage.queries import get_query
from syncstorage.storage.sqlmappers import bso as _bso
from syncstorage.storage.sqlmappers import (tables, collections,
                                            get_bso_table_name, MAX_TTL,
                                            get_bso_table,
                                            get_bso_table_byindex)


_KB = float(1024)

# For efficiency, it's possible to use fixed pre-determined IDs for
# common collection names.  This is the canonical list of such names.
# Non-standard collections will be allocated IDs starting from the
# highest ID in this collection.
# XXX: some names here are incorrect; we need to change "client" to "clients"
# "key" to "keys" at some point.  See Bug 688623
# XXX: we need to reserve ids 100 and above for custom collections, so
# that there's room to add new standard collections as needed.
_STANDARD_COLLECTIONS = {1: 'client', 2: 'crypto', 3: 'forms', 4: 'history',
                         5: 'key', 6: 'meta', 7: 'bookmarks', 8: 'prefs',
                         9: 'tabs', 10: 'passwords'}

STANDARD_COLLECTIONS_NAMES = dict((value, key) for key, value in
                                   _STANDARD_COLLECTIONS.items())


def _int_now():
    return int(time())


class _CustomCompiler(SQLCompiler):
    """SQLAlchemy statement compiler to support DELETE with ORDER BY and LIMIT.

    The visit_delete() method of this class is mostly a verbatim copy of the
    method from SQLCompiler, but has extra logic to handle ORDER BY and LIMIT
    clauses on the delete statement.
    """

    def visit_delete(self, delete_stmt):
        self.stack.append({'from': set([delete_stmt.table])})
        self.isdelete = True

        text = "DELETE FROM " + self.preparer.format_table(delete_stmt.table)

        if delete_stmt._returning:
            self.returning = delete_stmt._returning
            if self.returning_precedes_values:
                text += " " + self.returning_clause(delete_stmt,
                                                    delete_stmt._returning)

        if delete_stmt._whereclause is not None:
            text += " WHERE " + self.process(delete_stmt._whereclause)

        if len(delete_stmt._order_by_clause) > 0:
            text += " ORDER BY " + self.process(delete_stmt._order_by_clause)

        if delete_stmt._limit is not None or delete_stmt._offset is not None:
            text += self.limit_clause(delete_stmt)

        if self.returning and not self.returning_precedes_values:
            text += " " + self.returning_clause(delete_stmt,
                                                delete_stmt._returning)

        self.stack.pop(-1)

        return text


class _DeleteOrderBy(Delete):
    """Custom Delete statement with ORDER BY and LIMIT support."""

    def __init__(self, table, whereclause, bind=None, returning=None,
                 order_by=None, limit=None, offset=None, **kwargs):
        Delete.__init__(self, table, whereclause, bind, returning, **kwargs)
        self._order_by_clause = ClauseList(*util.to_list(order_by) or [])
        self._limit = limit
        self._offset = offset

    @_generative
    def order_by(self, *clauses):
        self.append_order_by(*clauses)

    def append_order_by(self, *clauses):
        if len(clauses) == 1 and clauses[0] is None:
            self._order_by_clause = ClauseList()
        else:
            if getattr(self, '_order_by_clause', None) is not None:
                clauses = list(self._order_by_clause) + list(clauses)
            self._order_by_clause = ClauseList(*clauses)

    @_generative
    def limit(self, limit):
        self._limit = limit

    @_generative
    def offset(self, offset):
        self._offset = offset

    def _copy_internals(self, clone=_clone):
        self._whereclause = clone(self._whereclause)
        for attr in ('_order_by_clause',):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr)))

    def get_children(self, column_collections=True, **kwargs):
        children = Delete.get_children(column_collections, **kwargs)
        return children + [self._order_by_clause]

    def _compiler(self, dialect, **kw):
        return _CustomCompiler(dialect, self, **kw)


def _delete(table, whereclause=None, **kwargs):
    return _DeleteOrderBy(table, whereclause, **kwargs)


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

        # A per-user cache for collection metadata.
        # This is to avoid looking up the collection name <=> id mapping
        # in the database on every request.
        self._temp_cache = defaultdict(dict)

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
            logger.error(err)
            raise BackendError(str(exc))

    #
    # Users APIs
    #
    def _get_query(self, name, user_id):
        """Get the named pre-built query, sharding by user_id if necessary."""
        if self.shard:
            return get_query(name, user_id)
        return get_query(name)

    def _get_collection_id(self, user_id, collection_name, create=True):
        """Returns a collection id, given the name."""
        if (self.standard_collections and
            collection_name in STANDARD_COLLECTIONS_NAMES):
            return STANDARD_COLLECTIONS_NAMES[collection_name]

        # custom collection
        data = self.get_collection(user_id, collection_name,
                                   ['collectionid'], create)
        if data is None:
            return None

        return data['collectionid']

    def delete_storage(self, user_id):
        """Removes all user data"""
        for query in ('DELETE_USER_COLLECTIONS', 'DELETE_USER_BSOS'):
            query = self._get_query(query, user_id)
            self._safe_execute(query, user_id=user_id)
        # XXX see if we want to check the rowcount
        return True

    #
    # Collections APIs
    #

    def delete_collection(self, user_id, collection_name):
        """deletes a collection"""
        if not self.collection_exists(user_id, collection_name):
            return

        # removing items first
        self.delete_items(user_id, collection_name)

        # then the collection
        query = self._get_query('DELETE_USER_COLLECTION', user_id)
        return self._safe_execute(query, user_id=user_id,
                                  collection_name=collection_name)

    def collection_exists(self, user_id, collection_name):
        """Returns True if the collection exists"""
        query = self._get_query('COLLECTION_EXISTS', user_id)
        res = self._safe_execute(query, user_id=user_id,
                                 collection_name=collection_name)
        res = res.fetchone()
        return res is not None

    def set_collection(self, user_id, collection_name, **values):
        """Creates a collection"""
        # XXX values is not used for now because there are no values besides
        # the name
        if self.collection_exists(user_id, collection_name):
            return

        values['userid'] = user_id
        values['name'] = collection_name

        if self.standard_collections:
            ids = _STANDARD_COLLECTIONS.keys()
            min_id = max(ids) + 1
        else:
            min_id = 0

        # getting the max collection_id
        # XXX why don't we have an autoinc here ?
        # see https://bugzilla.mozilla.org/show_bug.cgi?id=579096
        next_id = -1
        while next_id < min_id:
            query = self._get_query('COLLECTION_NEXTID', user_id)
            max_ = self._safe_execute(query, user_id=user_id).first()
            if max_[0] is None:
                next_id = min_id
            else:
                next_id = max_[0] + 1

        # insertion
        values['collectionid'] = next_id
        query = insert(collections).values(**values)
        self._safe_execute(query, **values)
        return next_id

    def get_collection(self, user_id, collection_name, fields=None,
                       create=True):
        """Return information about a collection."""
        if fields is None:
            fields = [collections]
            field_names = collections.columns.keys()
        else:
            field_names = fields
            fields = [getattr(collections.c, field) for field in fields]

        query = select(fields, and_(collections.c.userid == user_id,
                                    collections.c.name == collection_name))
        res = self._safe_execute(query).first()

        # the collection is created
        if res is None and create:
            collid = self.set_collection(user_id, collection_name)
            res = {'userid': user_id, 'collectionid': collid,
                   'name': collection_name}
            if fields is not None:
                for key in res.keys():
                    if key not in field_names:
                        del res[key]
        else:
            # make this a single step
            res = dict([(key, value) for key, value in res.items()
                         if value is not None])
        return res

    def get_collections(self, user_id, fields=None):
        """returns the collections information """
        if fields is None:
            fields = [collections]
        else:
            fields = [getattr(collections.c, field) for field in fields]

        query = select(fields, collections.c.userid == user_id)
        return self._safe_execute(query).fetchall()

    def get_collection_names(self, user_id):
        """return the collection names for a given user"""
        query = self._get_query('USER_COLLECTION_NAMES', user_id)
        names = self._safe_execute(query, user_id=user_id)
        return [(res[0], res[1]) for res in names.fetchall()]

    def get_collection_timestamps(self, user_id):
        """return the collection names for a given user"""
        query = 'COLLECTION_STAMPS'
        query = self._get_query(query, user_id)
        res = self._safe_execute(query, user_id=user_id)
        try:
            return dict([(self._collid2name(user_id, coll_id), stamp)
                         for coll_id, stamp in res])
        finally:
            self._purge_cache(user_id)

    def _cache(self, user_id, name, func):
        user_cache = self._temp_cache[user_id]
        if name in user_cache:
            return user_cache[name]
        data = func()
        user_cache[name] = data
        return data

    def _purge_cache(self, user_id):
        self._temp_cache[user_id].clear()

    def _collid2name(self, user_id, collection_id):
        if (self.standard_collections and
            collection_id in _STANDARD_COLLECTIONS):
            return _STANDARD_COLLECTIONS[collection_id]

        # custom collections
        def _coll():
            data = self.get_collection_names(user_id)
            return dict(data)

        collections = self._cache(user_id, 'collection_names', _coll)
        return collections[collection_id]

    def get_collection_counts(self, user_id):
        """Return the collection counts for a given user"""
        query = self._get_query('COLLECTION_COUNTS', user_id)
        res = self._safe_execute(query, user_id=user_id,
                                 ttl=_int_now())
        try:
            return dict([(self._collid2name(user_id, collid), count)
                          for collid, count in res])
        finally:
            self._purge_cache(user_id)

    def get_collection_max_timestamp(self, user_id, collection_name):
        """Returns the max timestamp of a collection."""
        query = self._get_query('COLLECTION_MAX_STAMPS', user_id)
        collection_id = self._get_collection_id(user_id, collection_name)
        res = self._safe_execute(query, user_id=user_id,
                                 collection_id=collection_id)
        res = res.fetchone()
        stamp = res[0]
        return stamp

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.

        The size is the sum of stored payloads.
        """
        query = self._get_query('COLLECTIONS_STORAGE_SIZE', user_id)
        res = self._safe_execute(query, user_id=user_id,
                                 ttl=_int_now())
        try:
            return dict([(self._collid2name(user_id, col[0]),
                        int(col[1]) / _KB) for col in res])
        finally:
            self._purge_cache(user_id)

    #
    # Items APIs
    #
    def item_exists(self, user_id, collection_name, item_id):
        """Returns a timestamp if an item exists."""
        collection_id = self._get_collection_id(user_id, collection_name)
        query = self._get_query('ITEM_EXISTS', user_id)
        res = self._safe_execute(query, user_id=user_id,
                                 item_id=item_id, ttl=_int_now(),
                                 collection_id=collection_id)
        res = res.fetchone()
        if res is None:
            return None
        return res[0]

    def _get_bso_table(self, user_id):
        if self.shard:
            return get_bso_table(user_id, self.shardsize)
        return _bso

    def get_items(self, user_id, collection_name, fields=None, filters=None,
                  limit=None, offset=None, sort=None):
        """returns items from a collection

        "filter" is a dict used to add conditions to the db query.
        Its keys are the field names on which the condition operates.
        Its values are the values the field should have.
        It can be a single value, or a list. For the latter the in()
        operator is used. For single values, the operator has to be provided.
        """
        bso = self._get_bso_table(user_id)
        collection_id = self._get_collection_id(user_id, collection_name)
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

        if offset is not None and int(offset) > 0:
            query = query.offset(int(offset))

        res = self._safe_execute(query)
        return [BSO(line) for line in res]

    def get_item(self, user_id, collection_name, item_id, fields=None):
        """returns one item"""
        bso = self._get_bso_table(user_id)
        collection_id = self._get_collection_id(user_id, collection_name)
        if fields is None:
            fields = [bso]
        else:
            fields = [getattr(bso.c, field) for field in fields]
        where = self._get_query('ITEM_ID_COL_USER', user_id)
        query = select(fields, where)
        res = self._safe_execute(query, user_id=user_id,
                           item_id=item_id, collection_id=collection_id,
                           ttl=_int_now()).first()
        if res is None:
            return None

        return BSO(res)

    def _set_item(self, user_id, collection_name, item_id, **values):
        """Adds or update an item"""
        bso = self._get_bso_table(user_id)

        if 'ttl' not in values:
            values['ttl'] = MAX_TTL
        else:
            # ttl is provided in seconds, so we add it
            # to the current timestamp
            values['ttl'] += _int_now()

        modified = self.item_exists(user_id, collection_name, item_id)

        if 'payload' in values:
            values['payload_size'] = len(values['payload'])

        collection_id = self._get_collection_id(user_id,
                                                collection_name)

        if modified is None:   # does not exists
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

        self._safe_execute(query)

        if 'modified' in values:
            return values['modified']

        return modified

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
        if not self.standard_collections:
            self.set_collection(user_id, collection_name)

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

        values = {}
        values['collection'] = self._get_collection_id(user_id,
                                                       collection_name)
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
        res = self._safe_execute(sqltext(query), **values)
        return res.rowcount

    def delete_item(self, user_id, collection_name, item_id,
                    storage_time=None):
        """Deletes an item"""
        collection_id = self._get_collection_id(user_id, collection_name)
        query = self._get_query('DELETE_SOME_USER_BSO', user_id)
        res = self._safe_execute(query, user_id=user_id,
                                 collection_id=collection_id,
                                 item_id=item_id)
        return res.rowcount == 1

    def delete_items(self, user_id, collection_name, item_ids=None,
                     storage_time=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        collection_id = self._get_collection_id(user_id, collection_name)
        bso = self._get_bso_table(user_id)
        query = _delete(bso)
        where = [bso.c.userid == bindparam('user_id'),
                 bso.c.collection == bindparam('collection_id')]

        if item_ids is not None:
            where.append(bso.c.id.in_(item_ids))

        where = and_(*where)
        query = query.where(where)

        # XXX see if we want to send back more details
        # e.g. by checking the rowcount
        res = self._safe_execute(query, user_id=user_id,
                                 collection_id=collection_id)
        return res.rowcount > 0

    def get_total_size(self, user_id, recalculate=False):
        """Returns the total size in KB of a user storage.

        The size is the sum of stored payloads.
        """
        query = self._get_query('USER_STORAGE_SIZE', user_id)
        res = self._safe_execute(query, user_id=user_id,
                                 ttl=_int_now())
        res = res.fetchone()
        if res is None or res[0] is None:
            return 0.0
        return int(res[0]) / _KB

    def get_size_left(self, user_id, recalculate=False):
        """Returns the storage left for a user"""
        return self.quota_size - self.get_total_size(user_id, recalculate)
