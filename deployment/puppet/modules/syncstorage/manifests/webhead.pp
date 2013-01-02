
class syncstorage::webhead inherits syncstorage  {

  include mozsvcbase
  include nginx

  $master_secrets = hiera("master_secrets")
  $db_user = hiera("db_user")
  $db_password = hiera("db_password")

  $db_nodes = {
    "stage-syncstorage0.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage0",
    "stage-syncstorage1.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage1",
    "stage-syncstorage2.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage2",
    "stage-syncstorage3.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage3",
    "stage-syncstorage4.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage4",
    "stage-syncstorage5.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage5",
    "stage-syncstorage6.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage6",
    "stage-syncstorage7.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage7",
    "stage-syncstorage8.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage8",
    "stage-syncstorage9.services.mozilla.com" =>  "syncstorage-stage-db1.c9vxdyuyp2a1.us-east-1.rds.amazonaws.com/syncstorage9",
  }

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
    content => template("syncstorage/secrets.erb"),
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
