#!/bin/sh
rm -rf weaveserver/templates/*.py
bin/flake8 weaveserver
