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

    def tearDown(self):
        shutil.rmtree(self.storage_dir)

    def test_directories_created(self):
        for file in ['root', 'metafile', 'lockfile', 'tmpdir', 'storagedir']:
            self.assertTrue(os.path.exists(getattr(self.storage, file)))

    def test_locked(self):
        self.assertRaises(CASLocked, self.storage.lock)

    def test_unlock(self):
        self.storage.unlock()
        self.storage.lock()
        self.test_locked()
