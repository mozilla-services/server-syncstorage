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
"""
Basic tests to verify that the dispatching mechanism works.
"""
import base64
import json
import smtplib
from email import message_from_string

from recaptcha.client import captcha

from syncserver.tests.functional import support


class FakeSMTP(object):

    msgs = []

    def __init__(self, *args, **kw):
        pass

    def quit(self):
        pass

    def sendmail(self, sender, rcpts, msg):
        self.msgs.append((sender, rcpts, msg))


class FakeCaptchaResponse(object):

    is_valid = True


class TestUser(support.TestWsgiApp):

    def setUp(self):
        super(TestUser, self).setUp()
        # user auth token
        environ = {'Authorization': 'Basic %s' % \
                        base64.encodestring('tarek:tarek')}
        self.app.extra_environ = environ

        # we don't want to send emails for real
        self.old = smtplib.SMTP
        smtplib.SMTP = FakeSMTP

        # we don't want to call recaptcha either
        self.old_submit = captcha.submit
        captcha.submit = self._submit

    def tearDown(self):
        super(TestUser, self).tearDown()

        # setting back smtp and recaptcha
        smtplib.SMTP = self.old
        captcha.submit = self.old_submit

    def _submit(self, *args, **kw):
        return FakeCaptchaResponse()

    def test_invalid_token(self):
        environ = {'Authorization': 'FOooo baar'}
        self.app.extra_environ = environ
        self.app.get('/user/1.0/tarek/password_reset', status=401)

    def test_user_exists(self):
        res = self.app.get('/user/1.0/tarek')
        self.assertTrue(json.loads(res.body))

    def test_user_node(self):
        res = self.app.get('/user/1.0/tarek/node/weave')
        self.assertTrue(res.body, 'http://localhost')

    def test_password_reset(self):
        # making sure a mail is sent
        res = self.app.get('/user/1.0/tarek/password_reset')
        self.assertEquals(res.body, 'success')
        self.assertEquals(len(FakeSMTP.msgs), 1)

        # let's ask via the web form now
        res = self.app.get('/weave-password-reset')
        res.form['username'].value = 'tarek'
        res = res.form.submit()
        self.assertTrue('next 6 hours' in res)
        self.assertEquals(len(FakeSMTP.msgs), 2)

        # let's visit the link in the email
        msg = message_from_string(FakeSMTP.msgs[1][2]).get_payload()
        msg = base64.decodestring(msg)
        link = msg.split('\n')[2].strip()

        # let's try some bad links (unknown user)
        badlink = link.replace('tarek', 'joe')
        res = self.app.get(badlink)
        res.form['password'].value = 'p' * 8
        res.form['confirm'].value = 'p' * 8
        res = res.form.submit()
        self.assertTrue('unable to locate your account' in res)

        badlink = link.replace('username=tarek&', '')
        res = self.app.get(badlink)
        res.form['password'].value = 'p' * 8
        res.form['confirm'].value = 'p' * 8
        res = res.form.submit()
        self.assertTrue('Username not provided' in res)

        # let's call the real link, it's a form we can fill
        # let's try bad values
        # mismatch
        res = self.app.get(link)
        res.form['password'].value = 'mynewpassword'
        res.form['confirm'].value = 'badconfirmation'
        res = res.form.submit()
        self.assertTrue('do not match' in res)

        # weak password
        res = self.app.get(link)
        res.form['password'].value = 'my'
        res.form['confirm'].value = 'my'
        res = res.form.submit()
        self.assertTrue('at least 8' in res)

        # wrong key
        res = self.app.get(link[:-1] + 'X')
        res.form['password'].value = 'mynewpassword'
        res.form['confirm'].value = 'mynewpassword'
        res = res.form.submit()
        self.assertTrue('Key does not match with username' in res)

        # all good
        res = self.app.get(link)
        res.form['password'].value = 'mynewpassword'
        res.form['confirm'].value = 'mynewpassword'
        res = res.form.submit()
        self.assertTrue('Password successfully changed' in res)

    def test_create_user(self):
        # creating a user

        # the user already exists
        payload = {'email': 'tarek@ziade.org', 'password': 'x' * 9}
        payload = json.dumps(payload)
        self.app.put('/user/1.0/tarek', params=payload, status=400)

        # missing the password
        payload = {'email': 'tarek@ziade.org'}
        payload = json.dumps(payload)
        self.app.put('/user/1.0/tarek2', params=payload, status=400)

        # malformed e-mail
        payload = {'email': 'tarekziadeorg', 'password': 'x' * 9}
        payload = json.dumps(payload)
        self.app.put('/user/1.0/tarek2', params=payload, status=400)

        # weak password
        payload = {'email': 'tarek@ziade.org', 'password': 'x'}
        payload = json.dumps(payload)
        self.app.put('/user/1.0/tarek2', params=payload, status=400)

        # weak password #2
        payload = {'email': 'tarek@ziade.org', 'password': 'tarek2'}
        payload = json.dumps(payload)
        self.app.put('/user/1.0/tarek2', params=payload, status=400)

        # everything is there
        res = self.app.get('/user/1.0/tarek2')
        self.assertFalse(json.loads(res.body))

        payload = {'email': 'tarek@ziade.org', 'password': 'x' * 9,
                   'captcha-challenge': 'xxx',
                   'captcha-response': 'xxx'}
        payload = json.dumps(payload)
        res = self.app.put('/user/1.0/tarek2', params=payload)
        self.assertEquals(res.body, 'tarek2')

        res = self.app.get('/user/1.0/tarek2')
        self.assertTrue(json.loads(res.body))

    def test_change_email(self):

        # bad email
        body = json.dumps('newemail.com')
        self.app.post('/user/1.0/tarek/email', params=body, status=400)

        # good one
        body = json.dumps('new@email.com')
        res = self.app.post('/user/1.0/tarek/email', params=body)
        self.assertEquals(res.body, 'new@email.com')

    def test_delete_user(self):
        # creating another user
        payload = {'email': 'tarek@ziade.org', 'password': 'x' * 9,
                   'captcha-challenge': 'xxx',
                   'captcha-response': 'xxx'}
        payload = json.dumps(payload)
        self.app.put('/user/1.0/tarek2', params=payload)

        # trying to suppress 'tarek' with 'tarek2'
        # this should generate a 401
        environ = {'Authorization': 'Basic %s' % \
                       base64.encodestring('tarek2:xxxxxxxxx')}
        self.app.extra_environ = environ
        self.app.delete('/user/1.0/tarek', status=401)

        # now using the right credentials
        environ = {'Authorization': 'Basic %s' % \
                       base64.encodestring('tarek:tarek')}
        self.app.extra_environ = environ
        res = self.app.delete('/user/1.0/tarek')
        self.assertTrue(json.loads(res.body))

        # tarek should be gone
        res = self.app.get('/user/1.0/tarek')
        self.assertFalse(json.loads(res.body))
