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
    """Table mapping (user_id, collection_name) => collection_id.

    This table provides a per-user namespace for custom collection names.
    It maps a (user_id, collection_name) pair to a unique collection id.
    """
    __tablename__ = 'collections'
    # XXX add indexes
    userid = Column(Integer(11), primary_key=True, nullable=False)
    collectionid = Column(Integer(6), primary_key=True, nullable=False)
    name = Column(String(32), nullable=False)


collections = Collections.__table__
tables.append(collections)


class _WBOBase(object):
    """Column definitions for sharded WBO storage.

    This mixin class defines the columns used for storage of WBO records.
    It is used to create either sharded or non-shareded WBO storage tables,
    depending on the run-time settings of the application.
    """
    id = Column(String(64), primary_key=True, autoincrement=False)
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11), nullable=False, default=0)
    ttl = Column(Integer(11), default=MAX_TTL)


#  If the storage controller is not doing sharding based on userid,
#  then it will use the single "wbo" table below for WBO storage.

class WBO(_WBOBase, _Base):
    """Table for storage of individual Weave Basic Object records.

    This table provides the (non-sharded) storage for WBO records along
    with their associated metadata.
    """
    __tablename__ = 'wbo'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

wbo = WBO.__table__


#  If the storage controller is doing sharding based on userid,
#  then it will use the below functions to select a table from "wbo0"
#  to "wboN" for each userid.

_SHARDS = {}


def get_wbo_table_byindex(index):
    if index not in _SHARDS:
        args = {'__tablename__': 'wbo%d' % index,
                '__table_args__':
                     {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}}
        klass = type('WBO%d' % index, (_WBOBase, _Base), args)
        _SHARDS[index] = klass.__table__
    return _SHARDS[index]


def get_wbo_table(user_id, shardsize=100):
    """Get the WBO table definition to use for the given user.

    This function determines the correct shard for the given userid and
    returns the definition for the matching WBO storage table.
    """
    return get_wbo_table_byindex(int(user_id) % shardsize)


def get_wbo_table_name(user_id, shardsize=100):
    """Get the name of WBO table to use for the given user.

    This function determines the correct shard for the given userid and
    returns the name of the matching WBO storage table.
    """
    return 'wbo%d' % (int(user_id) % shardsize)
