
class syncstorage::builder inherits syncstorage {

  Package["python26-syncstorage"] {
    require => Yumrepo["syncstorage"],
  }

  yumrepo { "syncstorage":
    baseurl => "file:///root/local-repo/x86_64",
    descr => "local repository of syncstorage rpms",
    enabled => 1,
    gpgcheck => 0,
    require => Exec["syncstorage_create_yumrepo"],
  }

  #
  #  The following is procedural code to checkout the repo and build the rpms
  #  for syncstorage and its dependencies.  The eventual result is a local
  #  yum repo in /root/local-repo, which can be used by the above resources.
  #
  #  Puppet is far from the ideal tool for managing this, but for now, it's
  #  not a bad place to start...
  #


  $syncstorage_repo = "https://github.com/mozilla-services/server-syncstorage"

  $syncstorage_build_deps = "gcc gcc-c++ git make python26-devel \
                             libevent-devel rpm-build createrepo"

  # Temporarily install the necessary build-time dependencies.
  # We don't do this with package{} because we want to remove them later.

  package { "python-setuptools":
    ensure => installed,
  }

  exec { "syncstorage_install_build_deps":
    command => "/usr/bin/yum --assumeyes install ${syncstorage_build_deps}",
    require => Package["python-setuptools"],
  }

  # Install the virtualenv package into system python.
  # Unfortunately this doesn't seem to be available as an RPM.

  exec { "syncstorage_install_virtualenv":
    command => "/usr/bin/easy_install virtualenv",
    creates => "/usr/bin/virtualenv",
    require => Exec["syncstorage_install_build_deps"],
    timeout => "0",
  }

  # Clone the git repo.

  exec { "syncstorage_clone_repo":
    command => "/usr/bin/git clone $syncstorage_repo",
    creates => "/root/server-syncstorage",
    cwd => "/root",
    require => Exec["syncstorage_install_virtualenv"],
    timeout => "0",
  }

  # Build the virtualenv inside the repo checkout.
  # XXX TODO: currently this builds latest dev version.

  exec { "syncstorage_build_venv":
    command => "/usr/bin/make build",
    cwd => "/root/server-syncstorage",
    creates => "/root/server-syncstorage/bin",
    require => Exec["syncstorage_clone_repo"],
    timeout => "0",
  }

  # Build the rpms.
  # XXX TODO: currently this builds latest dev version.

  exec { "syncstorage_build_rpms":
    command => "/usr/bin/make build_rpms",
    cwd => "/root/server-syncstorage",
    creates => "/root/server-syncstorage/rpms",
    require => Exec["syncstorage_build_venv"],
    timeout => "0",
  }

  exec { "syncstorage_create_yumrepo":
    command => "mkdir -p /root/local-repo/x86_64/RPMS && cp /root/server-syncstorage/rpms/*.rpm /root/local-repo/x86_64/RPMS/ && createrepo /root/local-repo/x86_64",
    creates => "/root/local-repo/x86_64/repodata/repomd.xml",
    path => "/bin:/usr/bin",
    require => Exec["syncstorage_build_rpms"],
    timeout => "0",
  }

  # Remove the build-time dependencies, we don't need them anymore.

  exec { "syncstorage_remove_build_files":
    command => "rm -rf /root/server-syncstorage",
    path => "/bin:/usr/bin",
    require => Exec["syncstorage_create_yumrepo"],
  }

  exec { "syncstorage_remove_build_deps":
    command => "/usr/bin/yum --assumeyes remove ${syncstorage_build_deps}",
    require => Exec["syncstorage_remove_build_files"],
  }

}
