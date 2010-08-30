.. _production-setup:

================
Production setup
================

XXX Explain here the different strategies to set up Sync in production
(apache, nginx)

Apache - mod_wsgi
=================

XXX Explain the vhost setup

Example::

    <Directory /path/to/sync>
      Order deny,allow
      Allow from all
    </Directory>


    <VirtualHost *:80>
      ServerName example.com
      DocumentRoot /path/to/sync
      WSGIProcessGroup sync.ziade.org
      WSGIDaemonProcess sync.ziade.org user=sync group=sync processes=2 threads=25
      WSGIPassAuthorization On
      WSGIScriptAlias / /path/to/sync/sync.wsgi
      CustomLog /var/log/apache2/sync.example.com-access.log combined
      ErrorLog  /var/log/apache2/sync.example.com-error.log
    </VirtualHost>


