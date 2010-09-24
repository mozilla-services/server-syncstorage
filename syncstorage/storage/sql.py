# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Sync Server
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2010
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Tarek Ziade (tarek@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
"""
SQL backend
"""
from time import time

from sqlalchemy import create_engine
from sqlalchemy.sql import (text, select, bindparam, delete, insert, update,
                            func, and_)

from syncstorage.storage.sqlmappers import (tables, users, collections,
                                            wbo, MAX_TTL)
from synccore.util import time2bigint, bigint2time
from syncstorage.wbo import WBO


_SQLURI = 'mysql://sync:sync@localhost/sync'
_STANDARD_COLLECTIONS = {1: 'client', 2: 'crypto', 3: 'forms', 4: 'history',
                         5: 'key', 6: 'meta', 7: 'bookmarks', 8: 'prefs',
                         9: 'tabs', 10: 'passwords'}

STANDARD_COLLECTIONS_NAMES = dict([(value, key) for key, value in
                                    _STANDARD_COLLECTIONS.items()])

# SQL Queries
_USER_N_COLL = and_(collections.c.userid == bindparam('user_id'),
                    collections.c.name == bindparam('collection_name'))

_USER_EXISTS = select([users.c.id], users.c.id == bindparam('user_id'))
_DELETE_USER_COLLECTIONS = delete(collections).where( \
                                 collections.c.userid == bindparam('user_id'))
_DELETE_USER_COLLECTION = delete(collections).where(_USER_N_COLL)
_DELETE_USER_WBOS = delete(wbo, wbo.c.username == bindparam('user_id'))
_DELETE_USER = delete(users, users.c.id == bindparam('user_id'))

_COLLECTION_EXISTS = select([collections.c.collectionid], _USER_N_COLL)

_COLLECTION_NEXTID = select([func.max(collections.c.collectionid)],
                            collections.c.userid == bindparam('user_id'))

_COLLECTION_STAMPS = select([wbo.c.collection, func.max(wbo.c.modified)],
            and_(wbo.c.username == bindparam('user_id'),
                 wbo.c.ttl > bindparam('ttl'))).group_by(wbo.c.collection)

_COLLECTION_COUNTS = select([wbo.c.collection, func.count(wbo.c.collection)],
           and_(wbo.c.username == bindparam('user_id'),
                wbo.c.ttl > bindparam('ttl'))).group_by(wbo.c.collection)

_COLLECTIONS_MAX_STAMPS = select([func.max(wbo.c.modified)],
            and_(wbo.c.collection == bindparam('collection_id'),
                 wbo.c.username == bindparam('user_id'),
                 wbo.c.ttl > bindparam('ttl')))

_ITEM_ID_COL_USER = and_(wbo.c.collection == bindparam('collection_id'),
                         wbo.c.username == bindparam('user_id'),
                         wbo.c.id == bindparam('item_id'),
                         wbo.c.ttl > bindparam('ttl'))

_ITEM_EXISTS = select([wbo.c.modified], _ITEM_ID_COL_USER)

_DELETE_ITEMS = delete(wbo,
                       and_(wbo.c.collection == bindparam('collection_id'),
                            wbo.c.username == bindparam('user_id'),
                            wbo.c.ttl > bindparam('ttl')))

_USER_STORAGE_SIZE = select([func.sum(wbo.c.payload_size)],
                       and_(wbo.c.username == bindparam('user_id'),
                            wbo.c.ttl > bindparam('ttl')))

_COLLECTIONS_STORAGE_SIZE = select([wbo.c.collection,
            func.sum(wbo.c.payload_size)],
            and_(wbo.c.username == bindparam('user_id'),
                 wbo.c.ttl > bindparam('ttl'))).group_by(wbo.c.collection)

_KB = float(1024)


class SQLStorage(object):

    def __init__(self, sqluri=_SQLURI, standard_collections=False,
                 use_quota=False, quota_size=0, pool_size=100,
                 pool_recycle=3600, reset_on_return=True):
        self.sqluri = sqluri
        kw = {'pool_size': int(pool_size),
              'pool_recycle': int(pool_recycle),
              'logging_name': 'weaveserver'}

        if self.sqluri.startswith('mysql'):
            kw['reset_on_return'] = reset_on_return

        self._engine = create_engine(sqluri, **kw)
        for table in tables:
            table.metadata.bind = self._engine
            table.create(checkfirst=True)
        self._user_collections = {}
        self.engine_name = self._engine.name
        self.standard_collections = standard_collections
        self.use_quota = use_quota
        self.quota_size = long(quota_size)

    @classmethod
    def get_name(cls):
        """Returns the name of the storage"""
        return 'sql'

    #
    # Users APIs
    #

    def user_exists(self, user_id):
        """Returns true if the user exists."""
        res = self._engine.execute(_USER_EXISTS, user_id=user_id).fetchone()
        return res is not None

    def set_user(self, user_id, **values):
        """set information for a user. values contains the fields to set.

        If the user doesn't exists, it will be created."""
        values['id'] = user_id
        if not self.user_exists(user_id):
            query = insert(users).values(**values)
        else:
            query = update(users).where(users.c.id == user_id)
            query = query.values(**values)

        self._engine.execute(query)

    def get_user(self, user_id, fields=None):
        """Returns user information.

        If fields is provided, its a list of fields to return
        """
        if fields is None:
            fields = [users]
        else:
            fields = [getattr(users.c, field) for field in fields]

        query = select(fields, users.c.id == user_id)
        return self._engine.execute(query).first()

    def delete_user(self, user_id):
        """Removes a user (and all its data)"""
        for query in (_DELETE_USER_COLLECTIONS, _DELETE_USER_WBOS,
                      _DELETE_USER):
            self._engine.execute(query, user_id=user_id)

    def _get_collection_id(self, user_id, collection_name, create=True):
        """Returns a collection id, given the name."""
        if (self.standard_collections and
            collection_name in STANDARD_COLLECTIONS_NAMES):
            return STANDARD_COLLECTIONS_NAMES[collection_name]

        # custom collection
        data = self.get_collection(user_id, collection_name,
                                   ['collectionid'])
        if data is None:
            # we want to create it
            if not create:
                return None
            return self.set_collection(user_id, collection_name)

        return data['collectionid']

    def delete_storage(self, user_id):
        """Removes all user data"""
        for query in (_DELETE_USER_COLLECTIONS, _DELETE_USER_WBOS):
            self._engine.execute(query, user_id=user_id)
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
        return self._engine.execute(_DELETE_USER_COLLECTION, user_id=user_id,
                                    collection_name=collection_name)

    def collection_exists(self, user_id, collection_name):
        """Returns True if the collection exists"""
        res = self._engine.execute(_COLLECTION_EXISTS, user_id=user_id,
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

        # getting the max collection_id
        # XXX why don't we have an autoinc here ?
        # see https://bugzilla.mozilla.org/show_bug.cgi?id=579096
        max = self._engine.execute(_COLLECTION_NEXTID,
                                   user_id=user_id).first()
        if max[0] is None:
            next_id = 1
        else:
            next_id = max[0] + 1

        # insertion
        values['collectionid'] = next_id
        query = insert(collections).values(**values)
        self._engine.execute(query, **values)
        return next_id

    def get_collection(self, user_id, collection_name, fields=None):
        """Return information about a collection."""
        if fields is None:
            fields = [collections]
            field_names = collections.columns.keys()
        else:
            field_names = fields
            fields = [getattr(collections.c, field) for field in fields]

        query = select(fields, and_(collections.c.userid == user_id,
                                    collections.c.name == collection_name))
        res = self._engine.execute(query).first()

        # the collection is created
        if res is None:
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
        return self._engine.execute(query).fetchall()

    def get_collection_names(self, user_id):
        """return the collection names for a given user"""
        query = text('select collectionid, name from collections '
                     'where userid = :user_id')
        return [(res[0], res[1]) for res in
                self._engine.execute(query, user_id=user_id).fetchall()]

    def get_collection_timestamps(self, user_id):
        """return the collection names for a given user"""
        res = self._engine.execute(_COLLECTION_STAMPS,
                                   user_id=user_id, ttl=int(time()))
        return dict([(self._collid2name(user_id, coll_id), bigint2time(stamp))
                     for coll_id, stamp in res])

    def _collid2name(self, user_id, collection_id):
        if (self.standard_collections and
            collection_id in _STANDARD_COLLECTIONS):
            return _STANDARD_COLLECTIONS[collection_id]

        # custom collections
        if user_id not in self._user_collections:
            names = dict(self.get_collection_names(user_id))
            self._user_collections[user_id] = names

        return self._user_collections[user_id][collection_id]

    def _purge_user_collections(self, user_id):
        if user_id in self._user_collections:
            del self._user_collections[user_id]

    def get_collection_counts(self, user_id):
        """Return the collection counts for a given user"""
        res = self._engine.execute(_COLLECTION_COUNTS, user_id=user_id,
                                   ttl=int(time()))
        try:
            return dict([(self._collid2name(user_id, collid), count)
                         for collid, count in res])
        finally:
            self._purge_user_collections(user_id)

    def get_collection_max_timestamp(self, user_id, collection_name):
        """Returns the max timestamp of a collection."""
        collection_id = self._get_collection_id(user_id, collection_name)
        res = self._engine.execute(_COLLECTIONS_MAX_STAMPS, user_id=user_id,
                                  collection_id=collection_id, ttl=int(time()))
        res = res.fetchone()
        stamp = res[0]
        if stamp is None:
            return None
        return bigint2time(stamp)

    def get_collection_sizes(self, user_id):
        """Returns the total size in KB for each collection of a user storage.

        The size is the sum of stored payloads.
        """
        if not self.use_quota:
            return dict()
        res = self._engine.execute(_COLLECTIONS_STORAGE_SIZE, user_id=user_id,
                                   ttl=int(time()))
        return dict([(self._collid2name(user_id, col[0]), int(col[1]) / _KB)
                     for col in res])

    #
    # Items APIs
    #
    def item_exists(self, user_id, collection_name, item_id):
        """Returns a timestamp if an item exists."""
        collection_id = self._get_collection_id(user_id, collection_name)
        res = self._engine.execute(_ITEM_EXISTS, user_id=user_id,
                                  item_id=item_id,
                                  collection_id=collection_id, ttl=int(time()))
        res = res.fetchone()
        if res is None:
            return None
        return bigint2time(res[0])

    def get_items(self, user_id, collection_name, fields=None, filters=None,
                  limit=None, offset=None, sort=None):
        """returns items from a collection

        "filter" is a dict used to add conditions to the db query.
        Its keys are the field names on which the condition operates.
        Its values are the values the field should have.
        It can be a single value, or a list. For the latter the in()
        operator is used. For single values, the operator has to be provided.
        """
        collection_id = self._get_collection_id(user_id, collection_name)
        if fields is None:
            fields = [wbo]
        else:
            fields = [getattr(wbo.c, field) for field in fields]

        # preparing the where statement
        where = [wbo.c.username == user_id,
                 wbo.c.collection == collection_id]

        if filters is not None:
            for field, value in filters.items():
                field = getattr(wbo.c, field)

                operator, value = value
                if field.name == 'modified':
                    value = time2bigint(value)

                if isinstance(value, (list, tuple)):
                    value = [str(item) for item in value]
                    where.append(field.in_(value))
                else:
                    if operator == '=':
                        where.append(field == value)
                    elif operator == '<':
                        where.append(field < value)
                    elif operator == '>':
                        where.append(field > value)

        if filters is None or 'ttl' not in filters:
            where.append(wbo.c.ttl > int(time()))

        where = and_(*where)
        query = select(fields, where)

        if sort is not None:
            if sort == 'oldest':
                query = query.order_by(wbo.c.modified.asc())
            elif sort == 'newest':
                query = query.order_by(wbo.c.modified.desc())
            else:
                query = query.order_by(wbo.c.sortindex.desc())

        if limit is not None and int(limit) > 0:
            query = query.limit(int(limit))

        if offset is not None and int(offset) > 0:
            query = query.offset(int(offset))

        res = self._engine.execute(query)
        converters = {'modified': bigint2time}
        return [WBO(line, converters) for line in res]

    def get_item(self, user_id, collection_name, item_id, fields=None):
        """returns one item"""
        collection_id = self._get_collection_id(user_id, collection_name)
        if fields is None:
            fields = [wbo]
        else:
            fields = [getattr(wbo.c, field) for field in fields]

        query = select(fields, _ITEM_ID_COL_USER)
        res = self._engine.execute(query, user_id=user_id, item_id=item_id,
                                   collection_id=collection_id,
                                   ttl=int(time())).first()
        if res is None:
            return None

        return WBO(res, {'modified': bigint2time})

    def _set_item(self, user_id, collection_name, item_id, **values):
        """Adds or update an item"""
        if 'modified' in values:
            values['modified'] = time2bigint(values['modified'])

        if 'ttl' not in values:
            values['ttl'] = MAX_TTL
        else:
            values['ttl'] += int(time())

        modified = self.item_exists(user_id, collection_name, item_id)

        if self.use_quota and 'payload' in values:
            values['payload_size'] = len(values['payload'])

        collection_id = self._get_collection_id(user_id,
                                                collection_name)

        if modified is None:   # does not exists
            values['collection'] = collection_id
            values['id'] = item_id
            values['username'] = user_id
            query = insert(wbo).values(**values)
        else:
            if 'id' in values:
                del values['id']
            key = and_(wbo.c.id == item_id, wbo.c.username == user_id,
                       wbo.c.collection == collection_id)
            query = update(wbo).where(key).values(**values)

        self._engine.execute(query)

        if 'modified' in values:
            return bigint2time(values['modified'])

        return modified

    def set_item(self, user_id, collection_name, item_id, **values):
        """Adds or update an item"""
        if 'payload' in values and 'modified' not in values:
            values['modified'] = time()

        return self._set_item(user_id, collection_name, item_id, **values)

    def set_items(self, user_id, collection_name, items):
        """Adds or update a batch of items.

        Returns a list of success or failures.
        """
        if self.engine_name in ('sqlite', 'postgresql'):
            count = 0
            for item in items:
                if 'id' not in item:
                    continue
                item_id = item['id']
                self.set_item(user_id, collection_name, item_id, **item)
                count += 1
            return count

        # XXX See if SQLAlchemy knows how to do batch inserts
        # that's quite specific to mysql
        fields = ('id', 'parentid', 'predecessorid', 'sortindex', 'modified',
                  'payload', 'payload_size', 'ttl')

        query = 'insert into wbo (username, collection, %s) values ' \
                    % ','.join(fields)

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
                if field == 'modified' and value is not None:
                    value = time2bigint(value)
                values['%s%d' % (field, num)] = value

            if ('payload%d' % num in values and
                'modified%d' % num not in values):
                values['modified%d' % num] = time2bigint(time())

            if values.get('ttl%d' % num) is None:
                values['ttl%d' % num] = 2100000000
            else:
                values['ttl%d' % num] += int(time())

            if self.use_quota and 'payload%d' % num in values:
                size = len(values['payload%d' % num])
                values['payload_size%d' % num] = size

        query += ','.join(lines)

        # allowing updates as well
        query += (' on duplicate key update parentid = values(parentid),'
                  'predecessorid = values(predecessorid),'
                  'sortindex = values(sortindex),'
                  'modified = values(modified), payload = values(payload),'
                  'payload_size = values(payload_size),'
                  'ttl = values(ttl)')

        res = self._engine.execute(text(query), **values)
        return res.rowcount

    def delete_item(self, user_id, collection_name, item_id):
        """Deletes an item"""
        collection_id = self._get_collection_id(user_id, collection_name)
        query = text('delete from wbo where username = :user_id and '
                     'collection = :collection_id and id = :item_id')
        res = self._engine.execute(query, user_id=user_id,
                                 collection_id=collection_id, item_id=item_id)
        return res.rowcount == 1

    def delete_items(self, user_id, collection_name, item_ids=None,
                     filters=None, limit=None, offset=None, sort=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        collection_id = self._get_collection_id(user_id, collection_name)
        if item_ids is None:
            query = ('delete from wbo where username = :user_id and '
                     'collection = :collection_id')
        else:
            ids = ', '.join(['"%s"' % str(id_) for id_ in item_ids])
            query = ('delete from wbo where username = :user_id and '
                     'collection = :collection_id and id in (%s)' % ids)

        # preparing filters
        extra = []
        extra_values = {}
        if filters is not None:
            for field, value in filters.items():
                operator, value = value
                if field == 'modified':
                    value = time2bigint(value)

                if isinstance(value, (list, tuple)):
                    value = [str(item) for item in value]
                    extra.append('%s %s (%s)' % (field, operator,
                                 ','.join(value)))
                else:
                    extra.append('%s %s :%s' % (field, operator, field))
                    extra_values[field] = value

        if extra != []:
            query = '%s and %s' % (query, ' and '.join(extra))

        if sort is not None and self.engine_name != 'sqlite':
            if sort == 'oldest':
                query += " order by modified"
            elif sort == 'newest':
                query += " order by modified desc"
            elif sort == 'index':
                query += " order by sortindex desc"

        if self.engine_name != 'sqlite':
            if limit is not None and int(limit) > 0:
                query += ' limit %d' % limit

            if offset is not None and int(offset) > 0:
                query += ' offset %d' % offset

        # XXX see if we want to send back more details
        # e.g. by checking the rowcount
        res = self._engine.execute(text(query), user_id=user_id,
                                   collection_id=collection_id, **extra_values)
        return res.rowcount > 0

    def get_total_size(self, user_id, recalculate=False):
        """Returns the total size in KB of a user storage.

        The size is the sum of stored payloads.
        """
        if not self.use_quota:
            return 0.0
        res = self._engine.execute(_USER_STORAGE_SIZE, user_id=user_id,
                                   ttl=int(time()))
        res = res.fetchone()
        if res is None or res[0] is None:
            return 0.0
        return int(res[0]) / _KB

    def get_size_left(self, user_id, recalculate=False):
        """Returns the storage left for a user"""
        return self.quota_size - self.get_total_size(user_id, recalculate)
