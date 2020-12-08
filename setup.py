# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

def load_req(filename):
    """Load a pip style requirement file."""
    reqs = []
    with open(filename, "r") as file:
        for line in file.readlines():
            line = line.strip()
            if line.startswith("-r"):
                content = load_req(line.split(' ')[1])
                reqs.extend(content)
                continue
            reqs.append(line)
    return reqs

install_requires = load_req("requirements.txt")

entry_points = """
[paste.app_factory]
main = syncstorage:main
"""

version = "1.8.0"

setup(name='SyncStorage',
      version=version,
      packages=find_packages(),
      install_requires=install_requires,
      entry_points=entry_points,
      license='MPLv2.0',
      classifiers=[
          "Programming Language :: Python",
          "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
      ],
      package_data={
          "syncstorage.tests": ["*.ini"],
      },
      include_package_data=True)
