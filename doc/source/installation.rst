============
Installation
============

To install Sync, make sure you have Python 2.6 and Virtualenv installed on
your system, then run the *make all* command. It will collect all the
required bits, create a local environment and run the tests to make sure
everything will work::

    $ make all
    New python executable in ./bin/python2.6
    Also creating executable in ./bin/python
    Installing setuptools............done.
    Finished processing dependencies for WeaveServer==0.1
    .................................
    Ran 33 tests in 37.834s

    OK

Once this is done, you should be able to run Sync locally with the
built-in server::

    $ bin/paster serve development.ini
    Starting server in PID 23027.
    serving on 0.0.0.0:5000 view at http://127.0.0.1:5000

The next steps are to :ref:`configure <configuration>` your server and decide
:ref:`how to run it <production-setup>`.

