.. _configuration:

=============
Configuration
=============

The server configuration is located in the *sync* section of the ini file
used to launch the application. The default file is :file:`development.ini`,
but you can create a new file as long as you configure Paster or the 
wsgi-enabled server to use it.

General Options
===============

- **storage**: name of the plugin used to read and write data.

  Possible values are:

   - sql
   - redisql

- **storage.***: every value prefixed with the **storage** namespace will be
  used to instanciate the plugin.

- **auth**: name of the plugin used to autenticate users.

  Possible values are:

   - dummy
   - sql

- **auth.***: every value prefixed with the **auth** namespace will be used
  to instanciate the plugin.

- **smtp.host**: SMTP server used to send emails. Defaults to *localhost*.

- **smtp.port**: SMTP port. Defaults to *25*.

- **smtp.sender**: SMTP sender used for the from field.
  Defaults to *weave@mozilla.com*.

- **smtp.user**: SMTP user, if an authentication is required.

- **smtp.password**: SMTP password, if an authentication is required.

Plugin Configuration
====================

XXX describe options for each built-in plugin


Logging Configuration
=====================

XXX describe logging options




