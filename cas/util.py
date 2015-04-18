import uuid
import os
import errno
import hashlib

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

def checksum(filename, hash_func=hashlib.sha1, block_size=2**20):
    sum = hash_func()
    fd = open(filename)
    while True:
        data = fd.read(block_size)
        if not data:
            break
        sum.update(data)
    return sum.hexdigest()
