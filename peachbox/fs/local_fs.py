import peachbox.fs

class LocalFs(peachbox.fs.FileSystem):
    def __init__(self, mart=None):
        self.mart        = mart or 'local_fs'
        self.dwh_path    = '/tmp'
        self._url_prefix = None

    def ls(self, path):
        return self.ls_a()[0]

    def ls_d(self, path):
        return self.ls_a()[1]

    def ls_a(self, path):
        files = []
        dirs  = []
        for dirname, dirnames, filenames in os.walk(self.url(path)):
            for filename in filenames:
                files.append(os.path.join(dirname, filename))
            for subdirname in dirnames:
                dirs.append(os.path.join(dirname, subdirname))
        return files, dirs

    def rm_r(self, path):
        if os.path.exists(self.url(path)):
            shutil.rmtree(self.url(path))

    def url(self, path):
        return self.url_prefix() + path()

    def url_prefix(self, path):
        return self._url_prefix or self.dwh_path + '/' + self.mart + '/'

