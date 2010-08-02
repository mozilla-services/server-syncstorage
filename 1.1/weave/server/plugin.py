# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Sync Server
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2010
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Tarek Ziade (tarek@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
"""
Base plugin class with registration mechanism and configuration reading.
"""
import abc


def filter_params(namespace, data, replace_dot='_', splitchar='.'):
    """Keeps only params that starts with the namespace.

    Will also convert booleans representation
    """
    master_value = None
    params = {}
    for key, value in data.items():
        if key == namespace:
            master_value = value
            continue
        if splitchar not in key:
            continue
        skey = key.split(splitchar)
        if skey[0] != namespace:
            continue
        if value.lower() in ('1', 'true'):
            value = True
        if value.lower() in ('0', 'false'):
            value = False
        params[replace_dot.join(skey[1:])] = value
    return master_value, params


class Plugin(object):
    """Abstract Base Class for plugins."""
    __metaclass__ = abc.ABCMeta
    name = ''

    @classmethod
    def get_from_config(cls, config):
        """Get a plugin from a config file."""
        storage_name, params = filter_params(cls.name, config)
        if storage_name is None:
            raise KeyError(cls.name)
        return cls.get(storage_name, **params)

    @classmethod
    def get(cls, name, **params):
        """Instanciates a plugin given its name"""
        for entry in cls._abc_registry:
            if entry.get_name() != name:
                continue
            return entry(**params)
        raise KeyError(name)

    @classmethod
    def __subclasshook__(cls, klass):
        for method in cls.__abstractmethods__:
            if any(method in base.__dict__ for base in klass.__mro__):
                continue
            raise TypeError('Missing "%s" in "%s"' % (method, klass))
        if klass not in cls._abc_registry:
            cls._abc_registry.add(klass)
        return True

    @abc.abstractmethod
    def get_name(self):
        """Returns the name of the plugin"""
