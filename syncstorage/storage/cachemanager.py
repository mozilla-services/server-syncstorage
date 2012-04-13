# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Memcached + SQL backend

- User tabs are stored in one single "user_id:tabs" key
- The total storage size is stored in "user_id:size"
- The meta/global bso is stored in "user_id"
"""
import threading
import thread

from pylibmc import Client, NotFound, ThreadMappedPool
from pylibmc import Error as MemcachedError

from pyramid.events import subscriber, NewRequest
from pyramid.threadlocal import get_current_registry

from mozsvc.exceptions import BackendError

from syncstorage.storage.sql import _KB

USER_KEYS = ('size', 'meta:global', 'tabs', 'stamps')


def _key(*args):
    return ':'.join([str(arg) for arg in args])


# Global list of CacheManager instances, so we can easily hook
# them up to pyramid events via a subscriber.
_instances = []


@subscriber(NewRequest)
def cleanup_pool_when_request_ends(event):
    """Whenever a request completes, clear the memcahed client pool.

    Unfortunately pyramid doesn't have a "RequestFinished" event.  Instead
    you are expected to subscribe to NewRequest and then add tasks to the
    list of "finished callbacks" for that each request as it is created.
    """
    for instance in _instances:
        event.request.add_finished_callback(instance._cleanup_pool)


class CacheManager(object):
    """ Helpers on the top of pylibmc
    """
    def __init__(self, *args, **kw):
        self._client = Client(*args, **kw)
        self.pool = ThreadMappedPool(self._client)
        # using a locker to avoid race conditions
        # when several clients for the same user
        # get/set the cached data
        self._locker = threading.RLock()
        _instances.append(self)
        self.logger = get_current_registry()['metlog']

    def _cleanup_pool(self, request):
        self.pool.pop(thread.get_ident(), None)

    def flush_all(self):
        with self.pool.reserve() as mc:
            mc.flush_all()

    def get(self, key):
        with self.pool.reserve() as mc:
            try:
                return mc.get(key)
            except MemcachedError, err:
                # memcache seems down
                raise BackendError(str(err))

    def delete(self, key):
        with self.pool.reserve() as mc:
            try:
                return mc.delete(key)
            except NotFound:
                return False
            except MemcachedError, err:
                # memcache seems down
                raise BackendError(str(err))

    def incr(self, key, size=1):
        size = int(size)
        with self.pool.reserve() as mc:
            try:
                return mc.incr(key, size)
            except NotFound:
                return mc.set(key, size)
            except MemcachedError, err:
                raise BackendError(str(err))

    def set(self, key, value):
        with self.pool.reserve() as mc:
            try:
                if not mc.set(key, value):
                    raise BackendError()
            except MemcachedError, err:
                raise BackendError(str(err))

    def get_set(self, key, func):
        res = self.get(key)
        if res is None:
            res = func()
            self.set(key, res)
        return res

    #
    # Tab managment
    #
    def get_tab(self, user_id, tab_id):
        tabs = self.get_tabs(user_id)
        if tabs is None:
            return None
        return tabs.get(tab_id)

    def get_tabs_size(self, user_id):
        """Returns the size of the tabs from memcached in KB"""
        tabs = self.get_tabs(user_id)
        size = sum([len(tab.get('payload', '')) for tab in tabs.values()])
        if size != 0:
            size = size / _KB
        return size

    def get_tabs_timestamp(self, user_id):
        """returns the max modified"""
        tabs_stamps = [tab.get('modified', 0)
                       for tab in self.get_tabs(user_id).values()]
        if len(tabs_stamps) == 0:
            return None
        return max(tabs_stamps)

    def _filter_tabs(self, tabs, filters):
        for field, value in filters.items():
            if field not in ('id', 'modified', 'sortindex'):
                continue

            operator, values = value

            # removing entries
            for tab_id, tab_value in tabs.items():
                if ((operator == 'in' and tab_id not in values) or
                    (operator == '>' and tab_value <= values) or
                    (operator == '<' and tab_value >= values)):

                    del tabs[tab_id]

    def get_tabs(self, user_id, filters=None):
        with self._locker:
            key = _key(user_id, 'tabs')
            tabs = self.get(key)
            if tabs is None:
                # memcached down ?
                tabs = {}
            if filters is not None:
                self._filter_tabs(tabs, filters)

        return tabs

    def set_tabs(self, user_id, tabs, merge=True):
        with self._locker:
            key = _key(user_id, 'tabs')
            if merge:
                existing_tabs = self.get(key)
                if existing_tabs is None:
                    existing_tabs = {}
            else:
                existing_tabs = {}
            for tab_id, tab in tabs.items():
                existing_tabs[tab_id] = tab
            self.set(key, existing_tabs)

    def delete_tab(self, user_id, tab_id):
        with self._locker:
            key = _key(user_id, 'tabs')
            tabs = self.get_tabs(user_id)
            if tab_id in tabs:
                del tabs[tab_id]
                self.set(key, tabs)
                return True
            return False

    def delete_tabs(self, user_id, item_ids=None):
        def _filter(tabs, filters, field, to_keep):
            operator, stamp = filters[field]
            if operator == '>':
                for tab_id, tab in list(tabs.items()):
                    if tab[field] <= stamp:
                        kept[tab_id] = tabs[tab_id]
            elif operator == '<':
                for tab_id, tab in list(tabs.items()):
                    if tab[field] >= stamp:
                        kept[tab_id] = tabs[tab_id]

        with self._locker:
            key = _key(user_id, 'tabs')
            kept = {}
            tabs = self.get(key)
            if tabs is None:
                # memcached down ?
                tabs = {}

            if item_ids is not None:
                for tab_id in list(tabs.keys()):
                    if tab_id not in item_ids:
                        kept[tab_id] = tabs[tab_id]
            self.set(key, kept)
            return len(kept) < len(tabs)

    def tab_exists(self, user_id, tab_id):
        tabs = self.get_tabs(user_id)
        if tabs is None:
            # memcached down ?
            return None
        if tab_id in tabs:
            return tabs[tab_id]['modified']
        return None

    #
    # misc APIs
    #
    def flush_user_cache(self, user_id):
        """Removes all cached data."""
        for key in USER_KEYS:
            try:
                self.delete(_key(user_id, key))
            except BackendError:
                self.logger.error('Could not delete user cache (%s)' % key)

    #
    # total managment
    #
    def set_total(self, user_id, total):
        # we store the size in bytes in memcached
        total = int(total * _KB)
        key = _key(user_id, 'size')
        # if this fail it's not a big deal
        try:
            self.set(key, total)
        except BackendError:
            self.logger.error('Could not write to memcached')

    def get_total(self, user_id):
        try:
            total = self.get(_key(user_id, 'size'))
            if total != 0 and total is not None:
                total = total / _KB
        except BackendError:
            total = None
        return total
