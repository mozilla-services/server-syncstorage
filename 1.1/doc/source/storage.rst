=======
Storage
=======

Firefox Sync Server can store user data in a SQL Database or in any kind of
backend, as long as you provide an implementation 

To write a new backend, you just have to implement a class that contains
all the methods described in the abstract class :class:`WeaveStorage`,
and register it::


    from weaveserver.storage import WeaveStorage

    class MySuperStorage(object):
        """Fine implementation"""

        @classmethod
        def get_name(cls):
            return 'superstorage'

    WeaveStorage.register(MySuperStorage)

Once this class is created and fully implements all methods, Sync can
be configured to use it in its ini sync section, via the 'storage' option::

    [sync]
    storage = superstorage

Sync will automatically instanciate the storage. If you need to pass some
options, use the storage namespace. Sync will pass it to the class 
constructor::

    [sync]
    storage = superstorage
    storage.option1 = foo


Here are the methods to implement:

.. autoclass:: weaveserver.storage.WeaveStorage
    :members:

