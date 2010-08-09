.. _authentication-plugins:

=========================================
Creating your own authentication back-end
=========================================

To write a new authentication back-end, you just have to implement a class
that contains all the methods described in the abstract class
:class:`WeaveAuth`::

    from weaveserver.auth import WeaveAuth

    class MySuperAuthenticator(object):
        """Fine implementation"""

        @classmethod
        def get_name(cls):
            return 'superauth'


Once this class is created and fully implements all methods, Sync can
be configured to use it in its ini sync section, via the *auth* option::

    [sync]
    auth = superauth.MySuperAuthenticator
    auth.param1 = one

In this example, the class is located in the :file:`superauth` module. 
See more details :ref:`here <plugin-registration>`.

Here are the methods to implement:

.. autoclass:: weaveserver.auth.WeaveAuth
    :members:

