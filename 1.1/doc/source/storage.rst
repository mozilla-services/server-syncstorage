.. _storage-plugins:

==================================
Creating your own storage back-end
==================================

Firefox Sync Server can store user data in a SQL Database or in any kind of
back-end, as long as you provide an implementation

To write a new back-end, you just have to implement a class that contains
all the methods described in the abstract class :class:`WeaveStorage`::


    class MySuperStorage(object):
        """Fine implementation"""

        @classmethod
        def get_name(cls):
            return 'superstorage'

        ... more code ...

.. _plugin-registration:

Once this class is created and fully implements all methods, Sync can
be configured to use it. Just point the fully qualified class name 
in the *storage* option of the *sync* section in the ini file::

    [sync]
    storage = foo.storage.SuperStorage

The fully qualified name will be used to import your class and instanciate 
it, so it should be reachable in the path. In the example, the class is
located in the *storage* module in the *foo* package.

If you need to pass some options, use the storage namespace. Sync 
will pass it to the class
constructor::

    [sync]
    storage = superstorage
    storage.option1 = foo


Here are the methods to implement:

.. autoclass:: weaveserver.storage.WeaveStorage
    :members:
