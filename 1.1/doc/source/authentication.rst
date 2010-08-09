.. _authentication-plugins:

=========================================
Creating your own authentication back-end
=========================================

To write a new authentication back-end, you just have to implement a class
that contains all the methods described in the abstract class
:class:`WeaveAuth`, and register it::

    from weaveserver.auth import WeaveAuth

    class MySuperAuthenticator(object):
        """Fine implementation"""

        @classmethod
        def get_name(cls):
            return 'superauth'

    WeaveAuth.register(MySuperAuthenticator)

Once this class is created and fully implements all methods, Sync can
be configured to use it in its ini sync section, via the *auth* option::

    [sync]
    auth = superauth

Sync will automatically instanciate the plugin. If you need to pass some
options, use the *auth* namespace. Sync will pass it to the class
constructor::

    [sync]
    auth = superauth
    auth.option1 = foo

Here are the methods to implement:

.. autoclass:: weaveserver.auth.WeaveAuth
    :members:

