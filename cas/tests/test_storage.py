import unittest
import tempfile
from cas import CAS, CASLocked
from cas.storage import SumIndex
import shutil
import os
import json
from mock import patch

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
        previous_updated = self.storage.updated
        sum = self.storage.add(self.testfile)

        self.assertFalse(previous_updated == self.storage.updated)
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
        previous_updated = self.storage.updated
        self.assertRaises(OSError, lambda: self.storage.remove(self.testfile))
        self.assertTrue(previous_updated == self.storage.updated)

    def test_successful_remove(self):
        previous_updated = self.storage.updated
        sum = self.storage.add(self.testfile)
        path = self.storage.path(sum)

        self.storage.remove(sum)

        self.assertFalse(previous_updated == self.storage.updated)
        self.assertFalse(os.path.isfile(path))
        self.assertFalse(os.path.isdir(os.path.dirname(path)))

    @patch('shutil.move')
    def test_add_failure(self, move):
        sum = self.storage.add(self.testfile)
        self.assertFalse(self.storage.has_sum(sum))
        self.storage.unlock()
        cas = CAS(self.storage_dir)

        # gc should've cleaned up this failed add
        self.assertFalse(bool(cas._sum_index.get(sum)))
        self.assertFalse(bool(cas._meta_index.has_sum(sum)))

class TestSumIndex(unittest.TestCase):
    def setUp(self):
        fdno, self.filename = tempfile.mkstemp()
        # reserve the name, but remove it so shelve doesn't think
        # it's an existing dbm
        os.remove(self.filename)
        self.index = SumIndex(self.filename)

    def tearDown(self):
        os.remove(self.filename)

    def test_unicode_sum(self):
        self.index.add(u'foo')
        self.assertTrue(self.index.has_key('foo'))
        self.index.remove(u'foo')
        self.assertFalse(self.index.has_key('foo'))
