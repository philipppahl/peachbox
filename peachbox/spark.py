from pyspark import SparkContext, SparkConf

class Spark(object):
    def __init__(self, app_name='', master='local', executor_memory='4g'):
        self.app_name        = app_name
        self.master          = master
        self.executor_memory = executor_memory
        self.sc              = None

    def context(self):
        if not self.sc: self.initialize()
        return self.sc

    def initialize(self):
        conf = SparkConf().setAppName(self.app_name)
        conf.setMaster(self.master)
        if self.master != 'local':
            conf.set("spark.executor.memory", "4g")
        self.sc = SparkContext(conf=conf)

    def stop(self):
        if self.sc:
            self.sc.stop()
            self.sc = None

