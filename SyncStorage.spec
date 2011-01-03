%define name python26-syncstorage
%define pythonname SyncStorage
%define version 0.1
%define unmangled_version 0.1
%define unmangled_version 0.1
%define release 11

Summary: Sync Storage server
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{pythonname}-%{unmangled_version}.tar.gz
License: MPL
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{pythonname}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Tarek Ziade <tarek@mozilla.com>
Requires: nginx memcached gunicorn python26 python26-memcached python26-setuptools python26-webob python26-paste python26-pastedeploy python26-services python26-pastescript python26-sqlalchemy python26-simplejson

Url: https://hg.mozilla.org/services/server-storage

%description
============
Sync Storage
============

This is the Python implementation of the Storage Server.


%prep
%setup -n %{pythonname}-%{unmangled_version} -n %{pythonname}-%{unmangled_version}

%build
python2.6 setup.py build

%install

# the config files for Sync apps
mkdir -p %{buildroot}%{_sysconfdir}/sync
install -m 0644 etc/sync.conf %{buildroot}%{_sysconfdir}/sync/sync.conf
install -m 0644 etc/production.ini %{buildroot}%{_sysconfdir}/sync/production.ini

# nginx config
mkdir -p %{buildroot}%{_sysconfdir}/nginx
mkdir -p %{buildroot}%{_sysconfdir}/nginx/conf.d
install -m 0644 etc/syncstorage.nginx.conf %{buildroot}%{_sysconfdir}/nginx/conf.d/syncstorage.conf

# logging
mkdir -p %{buildroot}%{_localstatedir}/log
touch %{buildroot}%{_localstatedir}/log/syncstorage.log

# the app
python2.6 setup.py install --single-version-externally-managed --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%post
touch %{_localstatedir}/log/syncstorage.log
chown nginx:nginx %{_localstatedir}/log/syncstorage.log
chmod 640 %{_localstatedir}/log/syncstorage.log

%files -f INSTALLED_FILES

%attr(640, nginx, nginx) %ghost %{_localstatedir}/log/syncstorage.log

%dir %{_sysconfdir}/sync/

%config(noreplace) %{_sysconfdir}/sync/*
%config(noreplace) %{_sysconfdir}/nginx/conf.d/syncstorage.conf

%defattr(-,root,root)
