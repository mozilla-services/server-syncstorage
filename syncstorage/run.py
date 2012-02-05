# -*- coding: utf8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Runs the Application. This script can be called by any wsgi runner that looks
for an 'application' variable
"""
import os
from logging.config import fileConfig
from ConfigParser import NoSectionError

# setting up the egg cache to a place where apache can write
os.environ['PYTHON_EGG_CACHE'] = '/tmp/python-eggs'

# setting up logging
ini_file = os.path.join('/etc', 'sync', 'production.ini')
try:
    fileConfig(ini_file)
except NoSectionError:
    pass

# running the app using Paste
from paste.deploy import loadapp
application = loadapp('config:%s' % ini_file)
