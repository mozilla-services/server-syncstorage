#!/bin/sh
#
#  Script to perform initial setup of a syncstorage webhead.
#
#  This script can be used via cloud-init to initialize a newly-provisioned
#  syncstorage webhead.  It contains just enough logic to checkout the
#  syncstorage repo and bootstrap into puppet, which takes care of most of
#  the actual setup and configuration.

WORK_DIR=/tmp/sync2-bootstrap-build

set -e

yum --assumeyes update

yum --assumeyes install puppet git

mkdir -p $WORK_DIR
cd $WORK_DIR

git clone https://github.com/mozilla-services/server-syncstorage
cd server-syncstorage
git checkout rfk/aws-deployment

cd deployment
puppet apply --modulepath=./puppet/modules ./puppet/webhead.pp

cd /
rm -rf $WORK_DIR
