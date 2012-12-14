%define name python26-syncstorage
%define pythonname SyncStorage
%define version 2.2
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
BuildRequires: libevent-devel
Requires: python26 python-setuptools python-pygments python26-argparse python26-cef python26-chameleon python26-cornice python26-docutils python26-gevent python26-greenlet python26-gunicorn python26-jinja2 python26-macauthlib python26-mako python26-markupsafe python26-metlog-py python26-mock python26-mozsvc python26-nose python26-ordereddict python26-paste python26-pastedeploy python26-pastescript python26-pybrowserid python26-pymysql python26-pymysql_sa python26-pyramid python26-pyramid_debugtoolbar python26-pyramid_macauth python26-repoze.lru python26-routes python26-simplejson python26-sphinx python26-sqlalchemy python26-tokenlib python26-translationstring python26-umemcache python26-unittest2 python26-venusian python26-webob python26-webtest python26-wsgiproxy python26-zope.deprecation python26-zope.interface
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
