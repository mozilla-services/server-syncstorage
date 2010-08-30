<%inherit file="base.mako"/>
<p>
 <strong>Note:</strong> Do not set this to be the same as your
 passphrase! If you are unsure what your passphrase is, you'll need
 to trigger a server wipe from the Weave add-on.</p>

 %if error:
 <div class="error">${error}</div>
 %endif
 <form class="mainForm" name="changePass" id="changePass"
    action="/weave-password-reset" method="post">
  <p>
   <label>New password:
    <input type="password" name="password" id="user_pass" size="20"/>
   </label>
  </p>
  <p>
   <label>Re-enter to confirm:
    <input type="password" name="confirm"
           id="user_pass2" size="20"/>
   </label>
  </p>
  <input type="hidden" name="key" value="${key}"/>
  %if username:
  <input type="hidden" name="username" value="${username}"/>
  %endif
  <input type="submit" id="pchange" name="pchange"
         value="Change my password"/>
 </form>
</p>
