<%inherit file="base.mako"/>

<p>Enter your username here and we'll send you an email with instructions and a key that will let you reset your password.</p>

<div class="box">
 <form class="mainForm" name="forgotPass" id="forgotPass"
       action="/weave-password-reset" method="post">
  <p>
   <label>Username:<br />
   <input type="text" name="username" id="user_login" size="20" /></label>
  </p>
  <p class="submit">
    <input type="submit" id="fpsubmit" value="Request Reset Key" />
  </p>
  <p>&nbsp;</p>
 </form>
</div>

