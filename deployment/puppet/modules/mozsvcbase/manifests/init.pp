
class mozsvcbase {

  Exec {
    path => "/usr/bin:/bin:/user/sbin:/sbin",
  }

  group { "puppet":
    ensure => "present",
  }

  ssh_authorized_key { "rfkelly":
    ensure => present,
    user => "ec2-user",
    type => dsa,
    key => "AAAAB3NzaC1kc3MAAACBAII4dY8YS+VJY5U9ktNmMCATQ1dGDAr0h7LHFZ/R986nQdsEEHYwt6F5YQAQLjXlOndbJT6YadVqZiVfeSMQy6Zk1xRH431YTZcyA1GKD3zkHSNUGg5U/Fjp6i2HINg0S7Hv0vXCRGZv5M7ViXf1k05gxNUiizudW/lLgDybnkAvAAAAFQD2jtKL2zUPCkkCYcWlpX/WoNlEewAAAIAOerQeatevI6FOdExH5e+a+LABZun8noGUMFwEpnCa+aJYFjrDHEUvfOezjVHhmjnyOqzlqCryxkf4kPMzlm78BXYMgMuRCXRkK6rQ8zXzYwaAxqIt+LKcwYciwmmxlFaTN1nq6w3DPg+WcPxVsHmYg57cwnb1aPTWN10fgNI5RwAAAIBfXYwqaAsEA+dHW6SKG64Mrp3k+mW4NdoHYfya/67k/BXUBsyJm485Xcth8LoCqiE1voIAQVNxXXCiTZSiTW9SKoqBdfijtUJtNcVlNWraJhEE1PcLyO9AR/JSnY3+xF+PULUFXc/OM+7WSYStb7PtBONEfF7tPO/S2IL6f5dNdQ=="
  }

}
