from cas.util import shard, get_uuid, mkdir_p, fullpath, checksum
import os
import json
import datetime
import errno
import shutil
import logging

LOG = logging.getLogger(__name__)

class CASLocked(RuntimeError): pass

class CAS(object):
    def __init__(self, root, sharding=(2, 2)):
        self.root = fullpath(root)
        self.shard_width, self.shard_depth = sharding

        self.uuid = None
        self.created = None
        self.updated = None

        self._initialize()

    def __del__(self):
        self.unlock()

    __exit__ = __del__

    def _initialize(self):
        self._initialize_dirs()
        self.lock()
        self._initialize_meta()

    def _initialize_dirs(self):
        map(mkdir_p, [self.tmpdir, self.storagedir])

    def _create_meta(self):
        self.uuid = get_uuid()
        self._update()
        self.created = self.updated
        self._write_meta()

    def _initialize_meta(self):
        if not os.path.isfile(self.metafile):
            self._create_meta()
        else:
            self._load_meta()

    def _update(self):
        self.updated = datetime.datetime.utcnow().isoformat()

    def _load_meta(self):
        meta = self._read_meta()
        self.uuid = meta['uuid']
        self.created = meta['created']
        self.updated = meta['updated']
        self.shard_width = meta['shard']['width']
        self.shard_depth = meta['shard']['depth']

    def _dump_meta(self):
        return {
          'uuid': self.uuid,
          'created': self.created, 
          'updated': self.updated, 
          'shard': { 
            'width': self.shard_width,
            'depth': self.shard_width,
          },
        }

    def _write_meta(self):
        json.dump(self._dump_meta(), open(self.metafile, 'w'))

    def _read_meta(self):
        return json.load(open(self.metafile)) 

    def lock(self):
        if self.locked:
            raise CASLocked(self.root)

        open(self.lockfile, 'a').close()

    def unlock(self):
        if self.locked:
            os.remove(self.lockfile)

    @property
    def locked(self):
        return os.path.isfile(self.lockfile)

    @property
    def metafile(self):
        return os.path.join(self.root, '.meta')

    @property
    def lockfile(self):
        return os.path.join(self.root, '.lock')

    @property
    def tmpdir(self):
        return os.path.join(self.root, '.tmp')

    @property
    def storagedir(self):
        return os.path.join(self.root, 'storage')

    def has_sum(self, sum):
        return os.path.isfile(self.path(sum))

    def has_file(self, filename):
        return self.has_sum(self.checksum(filename))

    def add(self, filename, verify=False):
        """
        Atomically add a file to storage
        """
        sum = self.checksum(filename)
        
        if self.has_sum(sum):
            # don't re-add a file that already exists
            return

        path = self.path(sum)
        destfile = os.path.basename(path)
        tmpfile = os.path.join(self.tmpdir, destfile)

        shutil.copy2(fullpath(filename), tmpfile)
      
        destdir = os.path.dirname(path)

        mkdir_p(destdir)

        shutil.move(tmpfile, destdir)

        self._update()
        self._write_meta()

        return sum

    def remove(self, sum):
        if not self.has_sum(sum):
            raise OSError(errno.ENOENT, sum)

        path = self.path(sum)
        os.remove(path)
        self._clean_dir(os.path.dirname(path))

        self._update()
        self._write_meta()

    def clean(self):
        """
        Perform garbage collection
        """
        pass

    def _clean_dir(self, dir):
        if os.path.isdir(dir) and not os.listdir(dir):
            shutil.rmtree(dir)

    def _shard(self, sum):
        return shard(sum, self.shard_width, self.shard_depth)        

    def path(self, sum):
        return os.path.join(self.storagedir, self._filename(sum))

    def _filename(self, sum):
        return os.path.sep.join(self._shard(sum))

    def checksum(self, filename):
        return checksum(filename)
