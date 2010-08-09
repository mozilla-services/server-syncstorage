=====================
Architecture overview
=====================

The Firefox Sync Server is a very simple WSGI application that implements
the `Sync <https://wiki.mozilla.org/Labs/Weave/Sync/1.0/API>`_ and
`User <https://wiki.mozilla.org/Labs/Weave/User/1.0/API>`_ APIs.


.. image:: sync.png
    :height: 400 px
    :width: 530 px

1. A request is made via a wsgi-compatible web server, like Apache/mod_wsgi
2. The :file:`weaveserver.wsgiapp` module is the entry point for incoming
   requests.
3. :class:`weaveserver.wsgiapp.SyncServerApp` authenticates the user when
   needed, using an authentication plugin.
4. The request is then dispatched to the right controller. Sync calls will
   go to the
   :class:`weaveserver.storagecontroller.StorageController` class and
   User calls to the :class:`weaveserver.usercontroller.UserController`
   class.
5. Both controllers can manipulate the data by using a storage plugin


Built-in plugins
================

The Firefox Sync Server provides built-in plugins for authentication and
storage.

- Authentication

 - *dummy*: a dummy plugin that always successfully authenticates. Useful for
   testing purposes.

 - *sql*: a SQL plugin that stores user information in a SQL database

- Storage

 - *sql*: a SQL plugin that stores everything in a SQL database

 - *redisql*: a SQL plugin that stores everything in a SQL database and
   uses Redis to cache some values to speed up the reads.

 - *multi*: a plugin that can be used to store data in several back-ends.


You can create your own back-ends see :ref:`storage-plugins` and
:ref:`authentication-plugins` sections.
