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
""" SQL Mappers
"""
from services.auth.sqlmappers import users

from sqlalchemy.ext.declarative import declarative_base, Column
from sqlalchemy import Integer, String, Text, BigInteger


_Base = declarative_base()
tables = []
MAX_TTL = 2100000000

# This is the table containing user data, which we import directly
# from the core authentication routines.
tables.append(users)


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
