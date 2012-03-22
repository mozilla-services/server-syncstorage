%define name python26-syncstorage
%define pythonname SyncStorage
%define version 2.0b1
%define release 1

Summary: Sync Storage server
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{pythonname}-%{version}.tar.gz
License: MPL
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{pythonname}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Ryan Kelly <rfkelly@mozilla.com>
BuildRequires: libevent-devel libmemcached-devel
Requires: memcached gunicorn python26 python26-argparse python26-cef python26-cornice python26-setuptools python26-docutils python26-gevent python26-greenlet python26-macauthlib python26-mako python26-markupsafe python26-metlog-py python26-mozsvc python26-mysql-python python26-ordereddict python26-paste python26-pastedeploy python26-pastescript python26-pylibmc python26-pymysql python26-pymysql_sa python26-pyramid python26-pyramid_debugtoolbar python26-pyramid_whoauth python26-repoze.lru python26-repoze.who python26-repoze.who.plugins.macauth  python26-pyzmq python-simplejson
Url: https://github.com/mozilla-servcices/server-syncstorage

%description
============
Sync Storage
============

This is the Python implementation of the SyncStorage Server.


%prep
%setup -n %{pythonname}-%{version} -n %{pythonname}-%{version}

%build
python2.6 setup.py build

%install
python2.6 setup.py install --single-version-externally-managed --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES

%defattr(-,root,root)
