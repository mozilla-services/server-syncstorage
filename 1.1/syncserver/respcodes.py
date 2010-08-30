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
""" 400 error codes
    See: https://wiki.mozilla.org/Labs/Weave/1.0/ResponseCodes
"""
WEAVE_ILLEGAL_METH = 1          # Illegal method/protocol
WEAVE_INVALID_CAPTCHA = 2       # Incorrect/missing captcha
WEAVE_INVALID_USER = 3          # Invalid/missing username
WEAVE_INVALID_WRITE = 4         # Attempt to overwrite data that can't be
WEAVE_WRONG_USERID = 5          # Userid does not match account in path
WEAVE_MALFORMED_JSON = 6        # Json parse failure
WEAVE_MISSING_PASSWORD = 7      # Missing password field
WEAVE_INVALID_WBO = 8           # Invalid Weave Basic Object
WEAVE_WEAK_PASSWORD = 9         # Requested password not strong enough
WEAVE_INVALID_RESET_CODE = 10   # Invalid/missing password reset code
WEAVE_UNSUPPORTED_FUNC = 11     # Unsupported function
WEAVE_NO_EMAIL_ADRESS = 12      # No email address on file
WEAVE_INVALID_COLLECTION = 13   # Invalid collection
WEAVE_OVER_QUOTA = 14           # User over quota
