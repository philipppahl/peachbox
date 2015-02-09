import peachbox.fs

import boto
from boto.s3.key import Key

class AmazonDfs(peachbox.fs.FileSystem):
    def __init__(self, mart=None):
        self.mart = mart
        self.connection = boto.connect_s3()

    def bucket(self):
        return self.connection.get_bucket(self.mart)

    def ls(self, path=''):
        return [self.url_prefix() + file.name for file in self.bucket().list(path)]

    def ls_d(self, path=''):
        return [self.url_prefix() + file.name.strip('/') for file in self.bucket().list(path+'/', '/')]

    def rm_r(self, path):
        if self.mart=='master' or path=="":
            raise "AmazonDfs.rm_r(): Can not delete data in master or whole bucket!"
            return
        for key in self.bucket().list(prefix=path): 
            key.delete() 

    def url_prefix(self):
        return 's3n://' + self.mart + '/'

