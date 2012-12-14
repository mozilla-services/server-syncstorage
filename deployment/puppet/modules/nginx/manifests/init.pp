
class nginx {

  package { "nginx":
    ensure => installed,
  }

  file { "nginx.conf":
      path => "/etc/nginx/nginx.conf",
      ensure => file,
      require => Package["nginx"],
      source => "puppet:///modules/nginx/nginx.conf",
      owner => "root",
      group => "root",
      mode => 0644,
  }

  service { "nginx":
      ensure => running,
      enable => true,
      subscribe => File["nginx.conf"],
      restart => "/sbin/service nginx reload",
  }

}
