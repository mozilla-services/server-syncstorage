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
from sqlalchemy.ext.declarative import declarative_base, Column
from sqlalchemy import Integer, String, DateTime, Text, BigInteger

_Base = declarative_base()
tables = []
MAX_TTL = 2100000000


class Collections(_Base):
    __tablename__ = 'collections'

    # XXX add indexes
    userid = Column(Integer(11), primary_key=True, nullable=False)
    collectionid = Column(Integer(6), primary_key=True, nullable=False)
    name = Column(String(32), nullable=False)

collections = Collections.__table__
tables.append(collections)


class Users(_Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, nullable=False)
    username = Column(String(32))
    password_hash = Column(String(128))
    email = Column(String(64))
    status = Column(Integer, default=0)
    alert = Column(Text)
    reset = Column(String(32))
    reset_expiration = Column(DateTime())


users = Users.__table__
tables.append(users)


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
tables.append(wbo)
