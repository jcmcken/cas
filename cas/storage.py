from cas.util import shard, get_uuid, mkdir_p, fullpath, checksum, timeit
from cas.config import CAS_ROOT
from cas.files import NullType
import os
import json
import datetime
import errno
import shutil
import shelve
import logging
import re

LOG = logging.getLogger(__name__)

class CASLocked(RuntimeError): pass

class SumIndex(shelve.DbfilenameShelf):
    def add(self, sum):
        LOG.debug('adding "%s" to sum index' % sum)
        self[str(sum)] = None

    def remove(self, sum):
        LOG.debug('removing "%s" from sum index' % sum)
        del self[str(sum)]

class MetaIndex(shelve.DbfilenameShelf):
    def __init__(self, *args, **kwargs):
        shelve.DbfilenameShelf.__init__(self, writeback=True, *args, **kwargs)

    def add(self, key, value, sum):
        LOG.debug('adding meta %s=%s for sum "%s"' % (key, value, sum))
        self.setdefault(key, {}).setdefault(value, {})

        self[key][value][str(sum)] = None

        self.sync()

    def remove(self, key, value, sum):
        LOG.debug('removing meta %s=%s for sum "%s"' % (key, value, sum))
        data_value = self.get(key, {}).get(value, None)

        if data_value is None:
            LOG.error('no such meta %s=%s for sum "%s"' % (key, value, sum))
            return

        del self[key][value][str(sum)]

        # garbage collection on valuespace
        if not self[key][value]:
            del self[key][value]

        # garbage collection on keyspace
        if not self[key]:
            del self[key]

        self.sync()

    def _find(self, sum):
        locations = []
        for key, valuespace in self.iteritems():
            for value, items in valuespace.iteritems():
                if sum in items:
                    locations.append((key, value))
        return locations

    def remove_all(self, sum):
        for key, value in self._find(sum):
            self.remove(key, value, sum)

    def equals(self, key, value):
        return sorted(self.get(str(key), {}).get(str(value), {}).keys())

    def match(self, key, value_regex):
        matches = set()
        for value, sums in self.get(str(key), {}).iteritems():
            if re.search(str(value_regex), value):
                map(matches.add, sums.keys())
        return sorted(list(matches))

    def keyspace(self):
        data = {}
        for key, valuespace in self.iteritems():
            data[key] = sorted(valuespace.keys())
        return data

    def has_sum(self, sum):
        return bool(self._find(sum))

class CAS(object):
    def __init__(self, root=None, sharding=(2, 2), autoload=True):
        root = root or CAS_ROOT
        if not root:
            raise TypeError('CAS requires a root directory')

        self.root = fullpath(root)
        self.shard_width, self.shard_depth = sharding

        self.uuid = None
        self.created = None
        self.updated = None

        self._sum_index = None
        self._meta_index = None
    
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
        self._meta_index = MetaIndex(self.meta_indexfile)

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
            'depth': self.shard_depth,
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
            self._sum_index.sync()
            self._meta_index.sync()
            os.remove(self.lockfile)

    @property
    def sum_indexfile(self):
        return os.path.join(self.root, '.files')

    @property
    def meta_indexfile(self):
        return os.path.join(self.root, '.filemeta')

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

    def equals(self, key, value):
        return self._meta_index.equals(key, value)

    def match(self, key, value_regex):
        return self._meta_index.match(key, value_regex)

    @timeit('cas.storage.CAS.gc')
    def gc(self, full=False):
        """
        Perform garbage collection
        """
        LOG.debug('performing garbage collection')
        
        LOG.debug('removing temporary files')
        for file in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, file))

        LOG.debug('cleaning up file checksum index')
        for sum in self._sum_index.keys():
            if not self.has_sum(sum):
                self._meta_index.remove_all(sum)
                self._sum_index.remove(sum)

    @timeit('cas.storage.CAS.add')
    def add(self, filename, type=NullType):
        """
        Atomically add a file to storage
        """
        sum = self.checksum(filename)
        
        if self.has_sum(sum):
            LOG.warn('skipping, storage already has checksum "%s"' % sum)
            # don't re-add a file that already exists
            return sum

        typed = type(filename)
        typed.verify()

        meta = typed.meta()
        type_str = typed.type

        path = self.path(sum)
        destfile = os.path.basename(path)
        tmpfile = os.path.join(self.tmpdir, destfile)
        full = fullpath(filename)

        LOG.debug('copying "%s" to "%s"' % (full, tmpfile))
        shutil.copy2(full, tmpfile)
      
        destdir = os.path.dirname(path)

        mkdir_p(destdir)

        # add sum to indices before moving file in place, because this is easier
        # to clean up if the add operation fails here
        self._sum_index.add(sum)
        self._meta_index.add('type', type_str, sum)
        for key, val in meta.iteritems():
            self._meta_index.add(key, val, sum)

        LOG.debug('moving "%s" to "%s"' % (tmpfile, destdir))
        shutil.move(tmpfile, destdir)

        self._update()
        self._write_meta()

        return sum

    @timeit('cas.storage.CAS.remove')
    def remove(self, sum):
        if not self.has_sum(sum):
            raise OSError(errno.ENOENT, sum)

        path = self.path(sum)
        os.remove(path)

        # the reverse of add, if the remove fails, the indices never get
        # updated, so nothing to clean up
        self._meta_index.remove_all(sum)
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
