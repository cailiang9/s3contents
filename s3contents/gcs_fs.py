import os
import six
import time
import gcsfs
import tensorflow as tf

from s3contents.compat import FileNotFoundError
from s3contents.ipycompat import Unicode
from s3contents.genericfs import GenericFS, NoSuchFile
from collections import OrderedDict
from notebook.utils import is_file_hidden
from base64 import encodebytes, decodebytes
LARGEFSIZE = 8*1024**2

class OrderedDictCache(OrderedDict):
    'Store items in the order the keys were last added'

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        OrderedDict.__setitem__(self, key, value)
        if OrderedDict.__len__(self) > 1000:
            print('popitem')
            self.popitem(False)

class GFFS(GenericFS):
    project = Unicode(
        help="GFile Project", allow_none=True, default_value=None).tag(
            config=True, env="JPYNB_GCS_PROJECT")
    region_name = Unicode(
        "us-east-1", help="Region name").tag(
            config=True, env="JPYNB_GCS_REGION_NAME")

    prefix = Unicode("", help="Prefix path inside the specified bucket").tag(config=True)
    separator = Unicode("/", help="Path separator").tag(config=True)

    dir_keep_file = Unicode(
        "", help="Empty file to create when creating directories").tag(config=True)

    def __init__(self, log, **kwargs):
        super(GFFS, self).__init__(**kwargs)
        self.log = log
        self.fs = tf.io.gfile
        self.dstat = OrderedDictCache()
        self.init()

    def init(self):
        self.mkdir("")
        self.ls("")
        assert self.isdir(""), "The root directory should exists :)"

    #  GenericFS methods -----------------------------------------------------------------------------------------------

    def ls(self, path, contain_hidden = False):
        path_ = self.path(path)
        self.log.debug("S3contents.GFFS: Listing directory: `%s`", path_)
        files = [path+self.separator+f for f in self.fs.listdir(path_) if contain_hidden or not is_file_hidden(f)]
        return self.unprefix(files)

    def isfile(self, path):
        st = self.lstat(path)
        return st['type'] == 'file'

    def isdir(self, path):
        st = self.lstat(path)
        return st['type'] == 'directory'

    def mv(self, old_path, new_path):
        self.log.debug("S3contents.GFFS: Move file `%s` to `%s`", old_path, new_path)
        self.cp(old_path, new_path)
        self.rm(old_path)

    def cp(self, old_path, new_path):
        old_path_, new_path_ = self.path(old_path), self.path(new_path)
        self.log.debug("S3contents.GFFS: Coping `%s` to `%s`", old_path_, new_path_)

        if self.isdir(old_path):
            old_dir_path, new_dir_path = old_path, new_path
            subdirs = self.ls(old_dir_path, True)
            if subdirs:
                for obj in subdirs:
                    old_item_path = obj
                    new_item_path = old_item_path.replace(old_dir_path, new_dir_path, 1)
                    self.cp(old_item_path, new_item_path)
            else:
                self.fs.mkdir(new_path_)  # empty dir
        elif self.isfile(old_path):
            self.fs.copy(old_path_, new_path_)

    def rm(self, path):
        path_ = self.path(path)
        self.log.debug("S3contents.GFFS: Removing: `%s`", path_)
        if self.isfile(path):
            self.log.debug("S3contents.GFFS: Removing file: `%s`", path_)
            self.fs.remove(path_)
        elif self.isdir(path):
            self.log.debug("S3contents.GFFS: Removing directory: `%s`", path_)
            self.fs.rmtree(path_)

    def mkdir(self, path):
        path_ = self.path(path) #, self.dir_keep_file)
        self.log.debug("S3contents.GFFS: Making dir (touch): `%s`", path_)
        self.fs.makedirs(path_)

    def read(self, path, format = None):
        path_ = self.path(path)
        if not self.isfile(path):
            raise NoSuchFile(path_)
        with self.fs.GFile(path_, mode='rb') as f:
            if f.size() > LARGEFSIZE:
                def downchunk():
                    while True:
                        buf = f.read(n=1048576)
                        if not buf: break
                        yield buf
                return downchunk(), 'base64'
            bcontent = f.read()
        if format is None or format == 'text':
            # Try to interpret as unicode if format is unknown or if unicode
            # was explicitly requested.
            try:
                self.log.debug("S3contents.GFFS: read: `%s`", path_)
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                if format == 'text':
                    raise HTTPError(400, "%s is not UTF-8 encoded" % os_path, reason='bad format',)
        return encodebytes(bcontent).decode('ascii'), 'base64'

    def lstat(self, path):
        calltime = time.time()
        if path in self.dstat: 
            st = self.dstat[path]
            if calltime - st["calltime"] < 5:
                return st
        path_ = self.path(path)
        self.log.debug("S3contents.GFFS: lstat file: `%s` `%s`", path, path_)
        try:
            info = self.fs.stat(path_)
            self.dstat[path] = {"calltime":calltime, "ST_MTIME": info.mtime_nsec//1000000, 
                                "size": info.length, "type":"directory" if info.is_directory else "file"}
        except tf.errors.NotFoundError:
            self.dstat[path] = {"calltime":calltime, "ST_MTIME": 0, "type":None}
        return self.dstat[path]

    def write(self, path, content, format = None, mode = 'wb'):
        path_ = self.path(self.unprefix(path))
        self.log.debug("S3contents.GFFS: Writing file: `%s`", path_)
        with self.fs.GFile(path_, mode=mode) as f:
            if format=='base64':
                b64_bytes = content.encode('ascii')
                f.write(decodebytes(b64_bytes))
            else:
                f.write(content.encode("utf-8"))

    #  Utilities -------------------------------------------------------------------------------------------------------

    def strip(self, path):
        if isinstance(path, six.string_types):
            return path.strip(self.separator)
        if isinstance(path, (list, tuple)):
            return list(map(self.strip, path))

    def join(self, *paths):
        paths = self.strip(paths)
        return self.separator.join(paths)

    def get_prefix(self):
        return self.prefix
    prefix_ = property(get_prefix)

    def unprefix(self, path):
        """Remove the self.prefix_ (if present) from a path or list of paths"""
        path = self.strip(path)
        if isinstance(path, six.string_types):
            path = path[len(self.prefix_):] if path.startswith(self.prefix_) else path
            path = path[1:] if path.startswith(self.separator) else path
            return path
        if isinstance(path, (list, tuple)):
            path = [p[len(self.prefix_):] if p.startswith(self.prefix_) else p for p in path]
            path = [p[1:] if p.startswith(self.separator) else p for p in path]
            return path

    def path(self, *path):
        """Utility to join paths including the bucket and prefix"""
        path = list(filter(None, path))
        path = self.unprefix(path)
        items = [self.prefix_] + path
        return self.join(*items)

class GCSFS(GenericFS):

    project = Unicode(
        help="GCP Project", allow_none=True, default_value=None).tag(
            config=True, env="JPYNB_GCS_PROJECT")
    token = Unicode(
        help="Path to the GCP token", allow_none=True, default_value=None).tag(
            config=True, env="JPYNB_GCS_TOKEN_PATH")

    region_name = Unicode(
        "us-east-1", help="Region name").tag(
            config=True, env="JPYNB_GCS_REGION_NAME")
    bucket = Unicode(
        "notebooks", help="Bucket name to store notebooks").tag(
            config=True, env="JPYNB_GCS_BUCKET")

    prefix = Unicode("", help="Prefix path inside the specified bucket").tag(config=True)
    separator = Unicode("/", help="Path separator").tag(config=True)

    dir_keep_file = Unicode(
        ".gcskeep", help="Empty file to create when creating directories").tag(config=True)

    def __init__(self, log, **kwargs):
        super(GCSFS, self).__init__(**kwargs)
        self.log = log

        token = os.path.expanduser(self.token)
        self.fs = gcsfs.GCSFileSystem(project=self.project, token=token)

        self.init()

    def init(self):
        self.mkdir("")
        self.ls("")
        assert self.isdir(""), "The root directory should exists :)"

    #  GenericFS methods -----------------------------------------------------------------------------------------------

    def ls(self, path):
        path_ = self.path(path)
        self.log.debug("S3contents.GCSFS: Listing directory: `%s`", path_)
        files = self.fs.ls(path_)
        return self.unprefix(files)

    def isfile(self, path):
        path_ = self.path(path)
        is_file = False

        exists = self.fs.exists(path_)
        if not exists:
            is_file = False
        else:
            try:
                # Info will fail if path is a dir
                self.fs.info(path_)
                is_file = True
            except FileNotFoundError:
                pass

        self.log.debug("S3contents.GCSFS: `%s` is a file: %s", path_, is_file)
        return is_file

    def isdir(self, path):
        # GCSFS doesnt return exists=True for a directory with no files so
        # we need to check if the dir_keep_file exists
        is_dir = self.isfile(path + self.separator + self.dir_keep_file)
        path_ = self.path(path)
        self.log.debug("S3contents.GCSFS: `%s` is a directory: %s", path_, is_dir)
        return is_dir

    def mv(self, old_path, new_path):
        self.log.debug("S3contents.GCSFS: Move file `%s` to `%s`", old_path, new_path)
        self.cp(old_path, new_path)
        self.rm(old_path)

    def cp(self, old_path, new_path):
        old_path_, new_path_ = self.path(old_path), self.path(new_path)
        self.log.debug("S3contents.GCSFS: Coping `%s` to `%s`", old_path_, new_path_)

        if self.isdir(old_path):
            old_dir_path, new_dir_path = old_path, new_path
            subdirs = self.ls(old_dir_path)
            if subdirs:
                for obj in subdirs:
                    old_item_path = obj
                    new_item_path = old_item_path.replace(old_dir_path, new_dir_path, 1)
                    self.cp(old_item_path, new_item_path)
            else:
                self.fs.copy(old_path_, new_path_)  # empty dir
        elif self.isfile(old_path):
            self.fs.copy(old_path_, new_path_)

    def rm(self, path):
        path_ = self.path(path)
        self.log.debug("S3contents.GCSFS: Removing: `%s`", path_)
        if self.isfile(path):
            self.log.debug("S3contents.GCSFS: Removing file: `%s`", path_)
            self.fs.rm(path_)
        elif self.isdir(path):
            self.log.debug("S3contents.GCSFS: Removing directory: `%s`", path_)
            files = self.fs.walk(path_)
            for f in files:
                self.fs.rm(f)

    def mkdir(self, path):
        path_ = self.path(path, self.dir_keep_file)
        self.log.debug("S3contents.GCSFS: Making dir (touch): `%s`", path_)
        self.fs.touch(path_)

    def read(self, path):
        path_ = self.path(path)
        if not self.isfile(path):
            raise NoSuchFile(path_)
        with self.fs.open(path_, mode='rb') as f:
            content = f.read().decode("utf-8")
        return content

    def lstat(self, path):
        path_ = self.path(path)
        info = self.fs.info(path_)
        ret = {}
        ret["ST_MTIME"] = info["updated"]
        return ret

    def write(self, path, content, format):
        path_ = self.path(self.unprefix(path))
        self.log.debug("S3contents.GCSFS: Writing file: `%s`", path_)
        with self.fs.open(path_, mode='wb') as f:
            if format=='base64':
                b64_bytes = content.encode('ascii')
                f.write(decodebytes(b64_bytes))
            else:
                f.write(content.encode("utf-8"))

    #  Utilities -------------------------------------------------------------------------------------------------------

    def strip(self, path):
        if isinstance(path, six.string_types):
            return path.strip(self.separator)
        if isinstance(path, (list, tuple)):
            return list(map(self.strip, path))

    def join(self, *paths):
        paths = self.strip(paths)
        return self.separator.join(paths)

    def get_prefix(self):
        """Full prefix: bucket + optional prefix"""
        prefix = self.bucket
        if self.prefix:
            prefix += self.separator + self.prefix
        return prefix
    prefix_ = property(get_prefix)

    def unprefix(self, path):
        """Remove the self.prefix_ (if present) from a path or list of paths"""
        path = self.strip(path)
        if isinstance(path, six.string_types):
            path = path[len(self.prefix_):] if path.startswith(self.prefix_) else path
            path = path[1:] if path.startswith(self.separator) else path
            return path
        if isinstance(path, (list, tuple)):
            path = [p[len(self.prefix_):] if p.startswith(self.prefix_) else p for p in path]
            path = [p[1:] if p.startswith(self.separator) else p for p in path]
            return path

    def path(self, *path):
        """Utility to join paths including the bucket and prefix"""
        path = list(filter(None, path))
        path = self.unprefix(path)
        items = [self.prefix_] + path
        return self.join(*items)
