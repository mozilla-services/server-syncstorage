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

# the ini file is grabbed at its production place
# unless force via an environ variable
ini_file = os.path.join('/etc', 'syncserver', 'production.ini')
ini_file = os.path.abspath(os.environ.get('SYNCSTORAGE_INI_FILE', ini_file))

# running the app using Paste
if __name__ == '__main__':
    # setting up logging
    try:
        fileConfig(ini_file)
    except NoSectionError:
        pass

    from paste.deploy import loadapp
    application = loadapp('config:%s' % ini_file)
