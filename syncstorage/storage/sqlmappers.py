# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
""" SQL Mappers
"""

from sqlalchemy.ext.declarative import declarative_base, Column
from sqlalchemy import Integer, String, Text, BigInteger


_Base = declarative_base()
tables = []
MAX_TTL = 2100000000


class Collections(_Base):
    """Table mapping collection_name => collection_id.

    This table holds the names and corresponding ids of the collections in
    use on the storage node.
    """
    __tablename__ = 'collections'
    # XXX add indexes
    collectionid = Column(Integer(6), primary_key=True, nullable=False,
                          autoincrement=True)
    name = Column(String(32), nullable=False, unique=True)


collections = Collections.__table__
tables.append(collections)


class _BSOBase(object):
    """Column definitions for sharded BSO storage.

    This mixin class defines the columns used for storage of BSO records.
    It is used to create either sharded or non-shareded BSO storage tables,
    depending on the run-time settings of the application.
    """
    id = Column(String(64), primary_key=True, autoincrement=False)
    userid = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11), nullable=False, default=0)
    ttl = Column(Integer(11), default=MAX_TTL)


#  If the storage controller is not doing sharding based on userid,
#  then it will use the single "bso" table below for BSO storage.

class BSO(_BSOBase, _Base):
    """Table for storage of individual Basic Storage Object records.

    This table provides the (non-sharded) storage for BSO records along
    with their associated metadata.
    """
    __tablename__ = 'bso'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

bso = BSO.__table__


#  If the storage controller is doing sharding based on userid,
#  then it will use the below functions to select a table from "bso0"
#  to "bsoN" for each userid.

_SHARDS = {}


def get_bso_table_byindex(index):
    if index not in _SHARDS:
        args = {'__tablename__': 'bso%d' % index,
                '__table_args__':
                     {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}}
        klass = type('BSO%d' % index, (_BSOBase, _Base), args)
        _SHARDS[index] = klass.__table__
    return _SHARDS[index]


def get_bso_table(user_id, shardsize=100):
    """Get the BSO table definition to use for the given user.

    This function determines the correct shard for the given userid and
    returns the definition for the matching BSO storage table.
    """
    return get_bso_table_byindex(int(user_id) % shardsize)


def get_bso_table_name(user_id, shardsize=100):
    """Get the name of BSO table to use for the given user.

    This function determines the correct shard for the given userid and
    returns the name of the matching BSO storage table.
    """
    return 'bso%d' % (int(user_id) % shardsize)
