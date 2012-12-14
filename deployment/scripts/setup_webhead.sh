#!/bin/sh
#
#  Script to perform initial setup of a syncstorage webhead.
#
#  This script can be used via cloud-init to initialize a newly-provisioned
#  syncstorage webhead.  It contains just enough logic to checkout the
#  syncstorage repo and bootstrap into puppet, which takes care of most of
#  the actual setup and configuration.

#  All errors are fatal.

set -e

#  Update and bootstrap the system to puppetizability.

YUM="yum --assumeyes"

$YUM --assumeyes update
$YUM --assumeyes install puppet git rubygems

$YUM install ruby-devel make gcc
gem install hiera hiera-puppet
ln -s /usr/lib/ruby/gems/1.8/gems/hiera-puppet-1.0.0 /usr/share/puppet/modules/hiera-puppet
$YUM remove ruby-devel make gcc

cat << EOF > /etc/puppet/hiera.yaml
:hierarchy:
    - common
:backends:
    - yaml
:yaml:
    :datadir: '/etc/puppet/hieradata'
EOF
mkdir -p /etc/puppet/hieradata

cat << EOF > /etc/puppet/hieradata/common.yaml
db_password: 'SYNCTWOPASSWORD'
EOF

#  Grab the latest puppet scripts from github, and apply.

WORK_DIR=/tmp/sync2-bootstrap-build

mkdir -p $WORK_DIR
cd $WORK_DIR

git clone https://github.com/mozilla-services/server-syncstorage
cd server-syncstorage
git checkout rfk/aws-deployment

cd deployment
puppet apply --modulepath ./puppet/modules:/usr/share/puppet/modules --execute "include syncstorage::webhead::builder"

# Remove all the build leftovers

cd /
rm -rf $WORK_DIR
