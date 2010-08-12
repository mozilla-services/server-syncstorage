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
import socket
import logging
from time import strftime


CEF = 60
logging.addLevelName('CEF', CEF)

_HOST = socket.gethostname()
_CEF_AUTH_FAILURE = 'AuthFail'
_CEF_CAPTCHA_FAILURE = 'CaptchaFail'
_CEF_OVERRIDE_FAILURE = 'InvalidAdmin'
_CEF_ACCOUNT_LOCKED = 'AccountLockout'
_CEF_FORMAT = ('%(date)s %(host)s CEF:%(version)s|%(vendor)s|%(product)s|'
               '%(device_version)s|%(signature)s|%(name)s|%(severity)s|'
               'cs1Label=requestClientApplication cs1=%(user_agent)s '
               'requestMethod=%(method)s request=%(url)s '
               'src=%(source)s dest=%(dest)s')


class WeaveLogger(logging.getLoggerClass()):
    """Custom logger that adds CEF-specific logs methods"""

    def _convert(self, data):
        """Escapes |"""
        return str(data).replace('|', r'\|')

    def cef_auth_failure(self, message, severity, request):
        """Creates a CEF record, and emit it."""
        signature = self._convert(_CEF_AUTH_FAILURE)
        name = self._convert(message)
        severity = self._convert(severity)

        data = {'severity': severity,
                'source': request.remote_addr,
                'method': request.method,
                'url': request.url,
                'dest': request.host,
                'user_agent': request.headers.get('User-Agent', 'none'),
                'signature': signature,
                'name': name,
                'version': 0,
                'vendor': 'mozilla',
                'device_version': '1.3',
                'product': 'weave',
                'host': _HOST,
                'date': strftime("%b %d %H:%M:%S")}

        # make sure we don't have a | anymore
        for key, value in data.items():
            value = str(value)
            pos = value.find('|')
            if pos == 0 or (pos > 0 and value[pos-1] != '\\'):
                msg = '"%s" cannot contain a "|" char: "%s"' % (key, value)
                raise ValueError(msg)

        self.log(CEF, _CEF_FORMAT, data)

logging.setLoggerClass(WeaveLogger)
logger = logging.getLogger('weaveserver')
