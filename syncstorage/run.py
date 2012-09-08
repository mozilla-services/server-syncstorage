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
from paste.deploy import loadapp

# setting up the egg cache to a place where apache can write
os.environ['PYTHON_EGG_CACHE'] = '/tmp/python-eggs'

# Use the .ini file specified in the environment if given.
# Otherwise use the production .ini file if available.
ini_file = os.environ.get("SYNCSTORAGE_INI_FILE")
if ini_file is None:
    ini_file = os.path.join('/etc', 'mozilla-services',
                            'syncstorage', 'production.ini')
    if not os.path.exists(ini_file):
        msg = "Config file %s not found; please set SYNCSTORAGE_INI_FILE to "\
              "the path of your desired config file."
        raise RuntimeError(msg % (ini_file,))
ini_file = os.path.abspath(ini_file)

# setting up logging
try:
    fileConfig(ini_file)
except NoSectionError:
    pass

# running the app using Paste
application = loadapp('config:%s' % ini_file)
