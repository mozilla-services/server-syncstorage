#!/bin/sh
if [ ! -d 'bin' ]; then
  virtualenv > /dev/null
  if [ $? -ne 0 ]; then
    wget http://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.4.9.tar.gz
    tar -xzvf virtualenv-1.4.9.tar.gz
    cd virtualenv-1.4.9
    python2.6 setup.py install
    cd ..
    rm -rf virtualenv*
  fi
  virtualenv --no-site-packages .
  bin/easy_install nose
  bin/easy_install coverage
  bin/easy_install flake8
  bin/python setup.py develop
fi

bin/nosetests -s --cover-html --cover-html-dir=html --with-coverage --cover-package=weaveserver weaveserver

