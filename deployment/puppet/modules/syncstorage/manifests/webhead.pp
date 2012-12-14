

class syncstorage::webhead inherits syncstorage  {

  include mozsvcbase
  include nginx

  $db_host = "sync2-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com"
  $db_user = "sync2"
  $db_password = hiera("db_password")

  user { "www-data":
    ensure => present,
  }

  service { "gunicorn-syncstorage":
    ensure => running,
    enable => true,
    subscribe => File["production.ini"],
    require => Package["python26-syncstorage"],
    restart => "/sbin/service gunicorn-syncstorage reload",
  }

  file { "/etc/mozilla-services":
    require => Package["python26-syncstorage"],
    ensure => directory,
    owner => "root",
    group => "root",
    mode => 0755,
  }

  file { "/etc/mozilla-services/syncstorage":
    ensure => directory,
    owner => "root",
    group => "root",
    mode => 0755,
  }

  file { "production.ini":
    path => "/etc/mozilla-services/syncstorage/production.ini",
    ensure => file,
    require => Package["python26-syncstorage"],
    content => template("syncstorage/production.ini.erb"),
    owner => "root",
    group => "root",
    mode => 0644,
  }

  file { "secrets":
    path => "/etc/mozilla-services/syncstorage/secrets",
    ensure => file,
    source => "puppet:///modules/syncstorage/secrets",
    owner => "root",
    group => "root",
    mode => 0644,
  }

  file { "gunicorn-syncstorage":
    path => "/etc/init.d/gunicorn-syncstorage",
    ensure => file,
    require => Package["python26-syncstorage"],
    source => "puppet:///modules/syncstorage/gunicorn-syncstorage",
    owner => "root",
    group => "root",
    mode => 0755,
  }

}
