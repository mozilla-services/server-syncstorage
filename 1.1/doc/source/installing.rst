===============
Installing Sync
===============

To install Sync, make sure you have Python 2.6 installed and in your path,
the run the :file:`tests.sh` script. It will collect all the required bits
and create a local environment, then run the tests::

    $ ./tests.sh
    New python executable in ./bin/python2.6
    Also creating executable in ./bin/python
    Installing setuptools............done.
    Finished processing dependencies for WeaveServer==0.1
    .................................
    Ran 33 tests in 37.834s

    OK

Once this is done, you should be able to run Sync locally with the
builtin server::

    $ bin/paste serve development.ini
    Starting server in PID 23027.
    serving on 0.0.0.0:5000 view at http://127.0.0.1:5000

