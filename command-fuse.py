import os
from os import path
import subprocess
import logging
from fusepy.fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import errno
import tempfile


MAX_SIZE_CACHE = 32

class ConvertCache(object):
    def __init__(self):
        self.counter = 0
        self.cache = {}
        self.lru = {}

    def has(self, n, update):
        r = n in self.cache
        if update:
            self.lru[n] = self.counter
            self.counter += 1
        return r

    def get(self, n):
        if n in self.cache:
            self.lru[n] = self.counter
            self.counter += 1
            return self.cache[n]

    def set(self, n, v):
        self.lru[n] = self.counter
        self.counter += 1
        self.cache[n] = v


    def pop1(self):
        latest = self.counter + 1
        for n,v in self.lru.items():
            if v < latest:
                latest = v
                to_pop = n
        val = self.cache[to_pop]
        del self.lru[to_pop]
        del self.cache[to_pop]
        return to_pop, val

class HEIFFuse(LoggingMixIn, Operations):
    def __init__(self, basedir, cachedir):
        self.basedir = basedir
        self.cachedir = cachedir
        self.cachedata = ConvertCache()
        self.getattrcache = {}
        self.pathtransform = {}
        self.counter = 0

    def readdir(self, pathname, fh):
        while pathname and pathname[0] == '/':
            pathname = pathname[1:]
        orig = os.listdir(path.join(self.basedir, pathname))
        ret = []
        for f in orig:
            if f.endswith('.heic'):
                nf = f[:-len('.heic')] + '.jpeg'
                self.pathtransform[nf] = path.join(self.basedir, pathname, f)
                ret.append(nf)
            else:
                ret.append(f)
        return ret

    def getattr(self, pathname, fh=None):
        if not self._is_passthru(pathname) and \
            pathname in self.getattrcache:
                st = self.getattrcache[pathname]
        else:
            diskpath = self._diskpath(pathname)
            if not self._is_passthru(pathname):
                self.getattrcache[pathname] = os.lstat(diskpath)
                st = self.getattrcache[pathname]
            else:
                st = os.lstat(diskpath)
        return dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
            'st_nlink', 'st_size', 'st_uid'))

    getxattr = None
    open = None

    def _is_passthru(self, pathname):
        return pathname.endswith('.heic')

    def _diskpath(self, pathname):
        while pathname and pathname[0] == '/':
            pathname = pathname[1:]
        if pathname in self.pathtransform:
            if not self.cachedata.has(pathname, True):
                if len(self.cachedata.lru) > MAX_SIZE_CACHE:
                    _,v = self.cachedata.pop1()
                    assert v.startswith(self.cachedir)
                    os.unlink(v)
                cached = path.join(self.cachedir, f'cache_{self.counter}.jpeg')
                subprocess.check_call([
                    'heif-convert',
                    self.pathtransform[pathname],
                    cached])
                self.counter += 1
                self.cachedata.set(pathname, cached)
            return self.cachedata.get(pathname)
        return path.join(self.basedir, pathname)

    def read(self, pathname, size, offset, fh):
        with open(self._diskpath(pathname), 'rb') as ifile:
            ifile.seek(offset, 0)
            return ifile.read(size)

    def readlink(self, _pathname):
        raise FuseOSError(errno.EIO)

    def rmdir(self, pathname):
        raise FuseOSError(errno.EIO)

    def unlink(self, pathname):
        raise FuseOSError(errno.EIO)

def main(argv):
    if len(argv) != 3:
        print('usage: {} <original> <mountpoint>'.format(argv[0]))
        from sys import exit
        exit(1)

    logging.getLogger('fuse.log-mixin').setLevel(logging.DEBUG)
    with tempfile.TemporaryDirectory() as tdir:
        FUSE(HEIFFuse(argv[1], tdir), argv[2], foreground=True, nothreads=True, encoding='utf-8', debug=True)

if __name__ == '__main__':
    print("THIS IS COMPLETELY EXPERIMENTAL SOFTWARE")
    print("IT MAY DELETE DATA\n")
    print("USE AT YOUR OWN RISK\n")
    import sys
    main(sys.argv)
