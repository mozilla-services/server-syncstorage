#!/bin/sh
rm -rf weaveserver/templates/*.py
bin/flake8 weaveserver
if [ $? -eq 0 ]; then
  echo SUCCESS
else
  echo FAILED
fi
