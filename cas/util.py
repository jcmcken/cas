import cas.log
import uuid
import os
import errno
import hashlib
from functools import wraps
import time
import logging
import imp
import glob

LOG = logging.getLogger(__name__)

def timeit(hint):
    def outer_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            results = func(*args, **kwargs)
            elapsed = time.time() - now
            LOG.debug('executing func "%s" took %.5f seconds' % (hint, elapsed))
            return results
        return wrapper
    return outer_wrapper

def load_plugin_file(filename):
    LOG.debug('attempting to load plugin file "%s"' % filename)
    full = fullpath(filename)
    try:
        plugin = imp.load_module('plugin', open(full), '', ('py', 'U', 1))
    except ImportError:
        LOG.exception('failed to load plugin from file "%s"' % filename)
        return 
    return plugin

@timeit('cas.util.load_plugin_dir')
def load_plugin_dir(directory):
    LOG.debug('attempting to load plugins from directory "%s"' % directory)
    for filename in glob.glob('%s/*.py' % directory):
        load_plugin_file(filename)

def get_uuid():
    return uuid.uuid4().hex

def shard(string, width, depth, remainder=True):
    pieces = [ string[(width*i):(width*(i+1))] for i in xrange(depth) ]

    if remainder:
        pieces.append(string[(width*depth):])

    return [ i for i in pieces if i ]

def mkdir_p(directory):
    try:
        os.makedirs(directory)
    except OSError, e:
        if e.errno != errno.EEXIST or not os.path.isdir(directory):
            raise e

def fullpath(filename):
    return os.path.realpath(os.path.expanduser(filename))

@timeit('cas.util.checksum')
def checksum(filename, hash_func=hashlib.sha1, block_size=2**20):
    sum = hash_func()
    fd = open(filename)
    while True:
        data = fd.read(block_size)
        if not data:
            break
        sum.update(data)
    return sum.hexdigest()
