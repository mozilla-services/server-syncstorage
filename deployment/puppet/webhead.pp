
group { "puppet":
  ensure => "present",
}

include nginx
include syncserver
