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
""" Base test class, with an instanciated app.
"""
import os
import unittest
from ConfigParser import RawConfigParser

from sqlalchemy import insert
from webtest import TestApp

from weaveserver.wsgiapp import make_app
from weaveserver.storage import WeaveStorage
from weaveserver.auth import WeaveAuth
from weaveserver.util import ssha
import weaveserver

_WEAVEDIR = os.path.dirname(weaveserver.__file__)
_TOPDIR = os.path.split(_WEAVEDIR)[0]
if 'WEAVE_MYSQL_TESTS' in os.environ:
    _INI_FILE = os.path.join(_TOPDIR, 'tests_%s.ini' % \
                             os.environ['WEAVE_TESTFILE'])
else:
    _INI_FILE = os.path.join(_TOPDIR, 'tests.ini')

class TestWsgiApp(unittest.TestCase):

    def setUp(self):
        # loading tests.ini
        cfg = RawConfigParser()
        cfg.read(_INI_FILE)
        config = dict(cfg.items('sync'))
        self.storage = WeaveStorage.get_from_config(config)
        self.sqlfile = self.storage.sqluri.split('sqlite:///')[-1]
        self.app = TestApp(make_app(config))

        # adding a user (sql)
        self.auth = WeaveAuth.get_from_config(config)
        self.auth.create_user('tarek', 'tarek', 'tarek@mozilla.con')
        self.user_id = self.auth.get_user_id('tarek')

    def tearDown(self):
        if os.path.exists(self.sqlfile):
            os.remove(self.sqlfile)
        else:
            self.auth._engine.execute('truncate users')
            self.auth._engine.execute('truncate collections')
            self.auth._engine.execute('truncate wbo')
