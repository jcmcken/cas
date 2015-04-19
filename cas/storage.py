from cas.util import shard, get_uuid, mkdir_p, fullpath, checksum
import os
import json
import datetime
import errno
import shutil
import shelve
import logging

LOG = logging.getLogger(__name__)

class CASLocked(RuntimeError): pass

class SumIndex(shelve.DbfilenameShelf):
    def add(self, sum):
        LOG.debug('adding "%s" to sum index' % sum)
        self[str(sum)] = None

    def remove(self, sum):
        LOG.debug('removing "%s" from sum index' % sum)
        del self[str(sum)]

class CAS(object):
    def __init__(self, root, sharding=(2, 2), autoload=True):
        self.root = fullpath(root)
        self.shard_width, self.shard_depth = sharding

        self.uuid = None
        self.created = None
        self.updated = None

        self._sum_index = None

        if autoload:
            self._initialize()

    def __del__(self):
        self.unlock()

    __exit__ = __del__

    @classmethod
    def check(cls, directory):
        """
        Hint at whether the supplied directory is possibly a CAS

        If possible, we want to avoid placing CAS storage into a directory
        that already has junk in it.
        """
        cas = CAS(directory, autoload=False)

        for dir in [cas.root, cas.storagedir, cas.tmpdir]:
            if not os.path.isdir(dir):
                return False    

        for file in [cas.metafile, cas.sum_indexfile]:
            if not os.path.isfile(file):
                return False

        return True

    def _initialize(self):
        self._initialize_dirs()
        self.lock()
        self._initialize_meta()
        self._initialize_indices()
        self.gc()

    def _initialize_indices(self):
        self._sum_index = SumIndex(self.sum_indexfile)

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
        old = self.updated
        self.updated = datetime.datetime.utcnow().isoformat()
        LOG.debug('updating storage timestamp from "%s" to "%s"' % (old, self.updated))

    def _load_meta(self):
        meta = self._read_meta()
        self.uuid = meta['uuid']
        self.created = meta['created']
        self.updated = meta['updated']
        self.shard_width = meta['shard']['width']
        self.shard_depth = meta['shard']['depth']

    def meta(self):
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
        LOG.debug('writing storage metadata')
        json.dump(self.meta(), open(self.metafile, 'w'))

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
    def sum_indexfile(self):
        return os.path.join(self.root, '.files')

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

    def gc(self, full=False):
        """
        Perform garbage collection
        """
        LOG.debug('performing garbage collection')
        
        LOG.debug('removing temporary files')
        for file in os.listdir(self.tmpdir):
            os.remove(file)

        LOG.debug('cleaning up file checksum index')
        for sum in self._sum_index.keys():
            if not self.has_sum(sum):
                self._sum_index.remove(sum)

    def add(self, filename, verify=False):
        """
        Atomically add a file to storage
        """
        sum = self.checksum(filename)
        
        if self.has_sum(sum):
            LOG.warn('skipping, storage already has checksum "%s"' % sum)
            # don't re-add a file that already exists
            return

        path = self.path(sum)
        destfile = os.path.basename(path)
        tmpfile = os.path.join(self.tmpdir, destfile)
        full = fullpath(filename)

        LOG.debug('copying "%s" to "%s"' % (full, tmpfile))
        shutil.copy2(full, tmpfile)
      
        destdir = os.path.dirname(path)

        mkdir_p(destdir)

        # add sum to index before moving file in place, because this is easier
        # to clean up if the add operation fails here
        self._sum_index.add(sum)

        LOG.debug('moving "%s" to "%s"' % (tmpfile, destdir))
        shutil.move(tmpfile, destdir)

        self._update()
        self._write_meta()

        return sum

    def remove(self, sum):
        if not self.has_sum(sum):
            raise OSError(errno.ENOENT, sum)

        path = self.path(sum)
        os.remove(path)

        # the reverse of add, this is easier to clean up if the file was removed
        # successfully, but the index remains (gc will catch it)
        self._sum_index.remove(sum)

        self._clean_dir(os.path.dirname(path))

        self._update()
        self._write_meta()

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

    def list(self):
        for key, val in self._sum_index.iteritems():
            yield key
