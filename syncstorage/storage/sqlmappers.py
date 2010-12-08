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

# mapper defined in services.auth
tables.append(users)


class Collections(_Base):
    __tablename__ = 'collections'

    # XXX add indexes
    userid = Column(Integer(11), primary_key=True, nullable=False)
    collectionid = Column(Integer(6), primary_key=True, nullable=False)
    name = Column(String(32), nullable=False)

collections = Collections.__table__
tables.append(collections)


class WBO(_Base):
    __tablename__ = 'wbo'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


wbo = WBO.__table__

# preparing the sharded tables
#
# XXX instead of an ugly cut-n-paste, I need to use
# http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName


class WBO0(_Base):
    __tablename__ = 'wbo0'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO1(_Base):
    __tablename__ = 'wbo1'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO2(_Base):
    __tablename__ = 'wbo2'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO3(_Base):
    __tablename__ = 'wbo3'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO4(_Base):
    __tablename__ = 'wbo4'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO5(_Base):
    __tablename__ = 'wbo5'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO6(_Base):
    __tablename__ = 'wbo6'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO7(_Base):
    __tablename__ = 'wbo7'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO8(_Base):
    __tablename__ = 'wbo8'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


class WBO9(_Base):
    __tablename__ = 'wbo9'
    __table_args__ = {'mysql_engine': 'InnoDB',
                      'mysql_charset': 'latin1'}

    id = Column(String(64), primary_key=True, autoincrement=False)
    # XXX that's user id in fact
    username = Column(Integer(11), primary_key=True, nullable=False)
    collection = Column(Integer(6), primary_key=True, nullable=False,
                        default=0)
    parentid = Column(String(64))
    predecessorid = Column(String(64))
    sortindex = Column(Integer(11))
    modified = Column(BigInteger(20))
    payload = Column(Text)
    payload_size = Column(Integer(11))
    ttl = Column(Integer(11), default=MAX_TTL)


shards = [cls.__table__ for cls in (WBO0, WBO1, WBO2, WBO3, WBO4, WBO5, WBO6,
                                    WBO7, WBO8, WBO9)]


def get_wbo_table(user_id):
    return shards[int(user_id) % 10]


def get_wbo_table_name(user_id):
    return 'wbo%d' % (int(user_id) % 10)
