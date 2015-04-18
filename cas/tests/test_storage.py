import unittest
import tempfile
from cas import CAS, CASLocked
import shutil
import os
import json

class TestStorage(unittest.TestCase):
    def setUp(self):
        self.storage_dir = tempfile.mkdtemp()
        self.storage = CAS(self.storage_dir)
        fdno, self.testfile = tempfile.mkstemp()
        self.checksum = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

    def tearDown(self):
        shutil.rmtree(self.storage_dir)
        os.remove(self.testfile)

    def test_directories_created(self):
        for file in ['root', 'metafile', 'lockfile', 'tmpdir', 'storagedir']:
            self.assertTrue(os.path.exists(getattr(self.storage, file)))

    def test_locked(self):
        self.assertRaises(CASLocked, self.storage.lock)

    def test_unlock(self):
        self.storage.unlock()
        self.storage.lock()
        self.test_locked()

    def test_meta_loaded(self, storage=None):
        sto = storage or self.storage
        for meta in ['uuid', 'created', 'updated']:
            print meta
            print sto.uuid
            print sto.created
            print sto.updated
            self.assertTrue(isinstance(getattr(sto, meta), basestring))
        for meta in ['shard_width', 'shard_depth']:
            self.assertTrue(isinstance(getattr(sto, meta), int))

    def test_unlock_on_del(self):
        self.storage.__del__()
        self.assertFalse(self.storage.locked)

    def test_unlock_on_exit(self):
        self.storage.__exit__()
        self.assertFalse(self.storage.locked)

    def test_load_existing_meta(self):
        self.storage.unlock()
        new_cas = CAS(self.storage_dir)
        self.test_meta_loaded(new_cas)

    def test_add(self):
        sum = self.storage.add(self.testfile)

        self.assertEquals(sum, self.checksum)
        self.assertTrue(self.storage.has_sum(sum))
        self.assertTrue(self.storage.has_file(self.testfile))

    def test_multi_add(self):
        sum = self.storage.add(self.testfile)
        path = self.storage.path(sum)

        mtime = os.stat(path).st_mtime

        self.storage.add(self.testfile)

        self.assertEquals(mtime, os.stat(path).st_mtime)

    def test_remove_miss(self):
        self.assertRaises(OSError, lambda: self.storage.remove(self.testfile))

    def test_successful_remove(self):
        sum = self.storage.add(self.testfile)
        path = self.storage.path(sum)

        self.storage.remove(sum)

        self.assertFalse(os.path.isfile(path))
        self.assertFalse(os.path.isdir(os.path.dirname(path)))
