# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import time
import threading
import traceback

from syncstorage.storage import (SyncStorage,
                                 ConflictError,
                                 ItemNotFoundError,
                                 CollectionNotFoundError)

_UID = 1
_PLD = '*' * 500


class StorageTestsMixin(object):

    def get_storage(self):
        raise NotImplementedError

    def test_storage_interace_is_implemented(self):
        assert isinstance(self.storage, SyncStorage)

    def test_items(self):
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'col')
        self.storage.set_items(_UID, 'col', [])
        self.assertRaises(ItemNotFoundError,
                          self.storage.get_item_timestamp, _UID, 'col', '1')

        self.storage.set_item(_UID, 'col', '1', {'payload': _PLD})
        res = self.storage.get_item(_UID, 'col', '1')
        self.assertEquals(res['payload'], _PLD)

        self.storage.set_item(_UID, 'col', '2', {'payload': _PLD})

        items = self.storage.get_items(_UID, 'col')["items"]
        self.assertEquals(len(items), 2)

        self.storage.delete_item(_UID, 'col', '1')
        items = self.storage.get_items(_UID, 'col')["items"]
        self.assertEquals(len(items), 1)

        self.storage.delete_collection(_UID, 'col')
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'col')

        self.storage.set_items(_UID, 'col', [{'id': 'o', 'payload': _PLD}])
        res = self.storage.get_item(_UID, 'col', 'o')
        self.assertEquals(res['payload'], _PLD)

    def test_batches(self):
        self.assertRaises(CollectionNotFoundError,
                          self.storage.get_items, _UID, 'col')
        self.storage.set_item(_UID, 'col', 'o', {'payload': 'trance'})

        batch = self.storage.create_batch(_UID, 'col')
        self.storage.append_items_to_batch(_UID, 'col', batch,
                                           [{'id': 'o',
                                             'payload': 'tweaked'}])
        self.storage.apply_batch(_UID, 'col', batch)
        res = self.storage.get_item(_UID, 'col', 'o')
        self.assertEquals(res['payload'], 'tweaked')

    def test_get_collection_timestamps(self):
        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})
        self.storage.set_item(_UID, 'col2', '1', {'payload': _PLD})

        timestamps = self.storage.get_collection_timestamps(_UID)
        names = timestamps.keys()
        self.assertTrue('col1' in names)
        self.assertTrue('col2' in names)
        col2ts = self.storage.get_collection_timestamp(_UID, 'col2')
        self.assertAlmostEquals(col2ts, timestamps['col2'])

        # check that when we have several users, the method
        # still returns the same timestamp for the first user
        # which differs from the second user
        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})
        self.storage.set_item(_UID, 'col2', '1', {'payload': _PLD})

        user1_timestamps = self.storage.get_collection_timestamps(_UID)
        user1_timestamps = user1_timestamps.items()
        user1_timestamps.sort()

        user2_timestamps = self.storage.get_collection_timestamps(2)
        user2_timestamps = user2_timestamps.items()
        user2_timestamps.sort()

        self.assertNotEqual(user1_timestamps, user2_timestamps)

    def test_storage_size(self):
        before = self.storage.get_total_size(_UID)
        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})
        self.storage.set_item(_UID, 'col1', '2', {'payload': _PLD})
        wanted = len(_PLD) * 2
        self.assertEquals(self.storage.get_total_size(_UID) - before, wanted)

    def test_ttl(self):
        self.storage.set_item(_UID, 'col1', '1', {'payload': _PLD})
        self.storage.set_item(_UID, 'col1', '2', {'payload': _PLD, 'ttl': 0})
        time.sleep(1.1)
        items = self.storage.get_items(_UID, 'col1')["items"]
        self.assertEquals(len(items), 1)
        items = self.storage.get_items(_UID, 'col1', ttl=-1)["items"]
        self.assertEquals(len(items), 2)

    def test_dashed_ids(self):
        id1 = 'ec1b7457-003a-45a9-bf1c-c34e37225ad7'
        id2 = '339f52e1-deed-497c-837a-1ab25a655e37'
        self.storage.set_item(_UID, 'col1', id1, {'payload': _PLD})
        self.storage.set_item(_UID, 'col1', id2, {'payload': _PLD * 89})
        items = self.storage.get_items(_UID, 'col1')["items"]
        self.assertEquals(len(items), 2)
        self.storage.delete_items(_UID, 'col1', [id1, id2])
        items = self.storage.get_items(_UID, 'col1')["items"]
        self.assertEquals(len(items), 0)

    def test_collection_locking_enforces_consistency(self):
        # Create the collection and get initial timestamp.
        bso = {"id": "TEST", "payload": _PLD}
        ts0 = self.storage.set_items(_UID, "col1", [bso])

        # Some events to coordinate action between the threads.
        read_locked = threading.Event()
        write_complete = threading.Event()

        # Somewhere to collection failures from subthreads.
        # Assertion errors don't bubble up automatically.
        failures = []

        def catch_failures(func):
            def catch_failures_wrapper(*args, **kwds):
                try:
                    return func(*args, **kwds)
                except Exception:
                    failures.append(sys.exc_info())
            return catch_failures_wrapper

        # A reader thread.  It locks the collection for reading, then
        # reads the timestamp twice in succession.  They should both
        # match the initial timestamp despite concurrent write thread.
        @catch_failures
        def reader_thread():
            with self.storage.lock_for_read(_UID, "col1"):
                read_locked.set()
                ts1 = self.storage.get_collection_timestamp(_UID, "col1")
                self.assertEquals(ts0, ts1)
                # Give the writer a chance to update the value.
                # It may be blocking on us though, so don't wait forever.
                write_complete.wait(timeout=1)
                ts2 = self.storage.get_collection_timestamp(_UID, "col1")
                self.assertEquals(ts1, ts2)
            # After releasing our read lock, the writer should complete.
            # Make sure its changes are visible to this thread.
            write_complete.wait()
            ts3 = self.storage.get_collection_timestamp(_UID, "col1")
            self.assertTrue(ts2 < ts3)

        # A writer thread.  It waits until the collection is locked for
        # read, then attempts to write-lock and update the collection.
        # This may block or raise a ConflictError, so it tries in a loop
        # until succeeding.
        @catch_failures
        def writer_thread():
            read_locked.wait()
            storage = self.storage
            while True:
                try:
                    with self.storage.lock_for_write(_UID, "col1"):
                        ts1 = storage.get_collection_timestamp(_UID, "col1")
                        self.assertEquals(ts0, ts1)
                        ts2 = storage.set_items(_UID, "col1", [bso])
                        self.assertTrue(ts1 < ts2)
                        break
                except ConflictError:
                    continue
            write_complete.set()
            # Check that our changes are visible outside of the lock.
            ts3 = storage.get_collection_timestamp(_UID, "col1")
            self.assertEquals(ts2, ts3)

        reader = threading.Thread(target=reader_thread)
        writer = threading.Thread(target=writer_thread)
        reader.start()
        writer.start()
        reader.join(10)
        writer.join(10)
        if reader.isAlive() or writer.isAlive():
            print>>sys.stderr, "TEST THREADS APPEAR TO BE DEADLOCKED"
            print>>sys.stderr, "\n"
            current_frames = sys._current_frames()
            rframe = current_frames.get(reader.ident)
            if rframe is not None:
                print>>sys.stderr, "READ THREAD TRACEBACK:"
                print>>sys.stderr, "".join(traceback.format_stack(rframe))
                print>>sys.stderr, "\n"
            wframe = current_frames.get(writer.ident)
            if wframe is not None:
                print>>sys.stderr, "WRITE THREAD TRACEBACK:"
                print>>sys.stderr, "".join(traceback.format_stack(wframe))
                print>>sys.stderr, "\n"
            read_locked.set()
            write_complete.set()
        for exc_type, exc_val, exc_tb in failures:
            raise exc_type, exc_val, exc_tb
        if reader.isAlive() or writer.isAlive():
            raise RuntimeError("Test threads appear to be deadlocked")
