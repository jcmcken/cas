import unittest
import os
import hashlib
from cas.util import *

SHARD_TESTS = [
  ('foobar', 2, 2, ['fo', 'ob', 'ar']),
  ('foobarbaz', 2, 2, ['fo', 'ob', 'arbaz']),
  ('fo', 2, 2, ['fo']),
]

class UtilTest(unittest.TestCase):
    def test_shards(self):
        for string, width, depth, output in SHARD_TESTS:
            self.assertEquals(shard(string, width, depth), output)

    def test_fullpath(self):
        self.assertTrue(fullpath('~/foo').startswith('/home'))
        self.assertTrue(fullpath('~/foo').endswith('/foo'))

    def test_mkdir_p(self):
        mkdir_p('/etc')
        self.assertTrue(os.path.isdir('/etc'))

    def test_checksum(self):
        self.assertEquals(hashlib.sha1(open('/etc/hosts').read()).hexdigest(), 
          checksum('/etc/hosts'))

    def test_mkdir_p_failure(self):
        self.assertRaises(OSError, lambda: mkdir_p('/etc/hosts'))

    def test_uuid(self):
        self.assertTrue(isinstance(get_uuid(), str))
