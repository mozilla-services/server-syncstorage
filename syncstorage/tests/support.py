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
from ConfigParser import RawConfigParser
import os
from logging.config import fileConfig

from syncstorage.storage import SyncStorage
from services.auth import ServicesAuth
from services.util import convert_config

_DIR = os.path.dirname(__file__)

while True:
    if 'WEAVE_TESTFILE' in os.environ:
        _INI_FILE = os.path.join(_DIR, 'tests_%s.ini' % \
                                 os.environ['WEAVE_TESTFILE'])
    else:
        _INI_FILE = os.path.join(_DIR, 'tests.ini')

    if os.path.exists(_INI_FILE):
        break

    _DIR = os.path.split(_DIR)[0]
    if _DIR == '/':
        raise IOError("could not find a test ini")


def initenv(config=_INI_FILE):
    """Reads the config file and instanciates an auth and a storage.

    The WEAVE_TESTFILE=name environment variable can be used to point
    a particular tests_name.ini file.
    """
    # pre-registering plugins
    from syncstorage.storage.sql import SQLStorage
    SyncStorage.register(SQLStorage)

    try:
        from syncstorage.storage.memcachedsql import MemcachedSQLStorage
        SyncStorage.register(MemcachedSQLStorage)
    except ImportError:
        pass
    from services.auth.sql import SQLAuth
    ServicesAuth.register(SQLAuth)
    try:
        from services.auth.ldapsql import LDAPAuth
        ServicesAuth.register(LDAPAuth)
    except ImportError:
        pass
    from services.auth.dummy import DummyAuth
    ServicesAuth.register(DummyAuth)

    cfg = RawConfigParser()
    cfg.read(config)

    # loading loggers
    if cfg.has_section('loggers'):
        fileConfig(_INI_FILE)

    here = {'here': os.path.dirname(os.path.realpath(config))}
    config = dict([(key, value % here)for key, value in
                   cfg.items('DEFAULT') + cfg.items('app:main')])
    config = convert_config(config)
    storage = SyncStorage.get_from_config(config, 'storage')
    auth = ServicesAuth.get_from_config(config, 'auth')
    return _DIR, config, storage, auth
