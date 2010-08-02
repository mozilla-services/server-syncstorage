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
Multiple backend -- read to a master storage,
writes to a collection of slave storages
"""
from weave.server.storage import WeaveStorage
from weave.server.plugin import filter_params


def _prepare_apis(name, bases, attrs):
    """Sets the APIs so reads happen on master, and write on all"""
    read = ('user_exists', 'get_user', 'collection_exists', 'get_collection',
            'get_collections', 'get_collection_names', 'get_item',
            'get_collection_timestamps', 'get_collection_counts',
            'get_collection_max_timestamp', 'item_exists', 'get_items')

    write = ('set_user', 'delete_user', 'delete_storage', 'delete_collection',
             'set_collection', 'set_item', 'set_items', 'delete_items',
             'delete_item')

    def _write(func):
        def __write(self, *args, **kwargs):
            res = getattr(self.master, func)(*args, **kwargs)
            # XXX see if we want to perform it asynced
            for slave in self.slaves:
                getattr(slave, func)(*args, **kwargs)
            return res
        return __write

    def _read(func):
        def __read(self, *args, **kwargs):
            return getattr(self.master, func)(*args, **kwargs)
        return __read

    for meth in read:
        attrs[meth] = _read(meth)

    for meth in write:
        attrs[meth] = _write(meth)

    return type(name, bases, attrs)


class WeaveMultiStorage(object):
    """Iterate on storages on every call."""
    __metaclass__ = _prepare_apis

    def __init__(self, master, slaves, **params):
        __, master_params = filter_params('master', params, splitchar='_')
        self.master = WeaveStorage.get(master, **master_params)
        self.slaves = []

        for slave in slaves.split(','):
            name, type_ = slave.split(':')
            __, slave_params = filter_params(name, params, splitchar='_')
            self.slaves.append(WeaveStorage.get(type_, **slave_params))

    @classmethod
    def get_name(cls):
        """Returns the name of the storage"""
        return 'multi'

WeaveStorage.register(WeaveMultiStorage)
