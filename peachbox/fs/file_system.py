import os

class FileSystem(object):

    def ls(self, path=''):
        NotImplementedError

    def ls_d(self, path=''):
        NotImplementedError

    def dirs_of_period(self, path, from_utime, before_utime):
        dirs       = self.ls_d(path)
        valid_dirs = []
        for dir in dirs:
            utime = self.dir_utime(dir)
            if utime >= from_utime and utime < before_utime:
                valid_dirs.append(dir)
        return valid_dirs

    def dir_utime(self, path):
        basename = os.path.basename(path)
        try:
            utime = int(dir)
            return utime
        except ValueError:
            raise 'FileSystem.dir_utime(): Name of directory does not contain a number'

