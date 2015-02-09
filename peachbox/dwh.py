# Copyright 2015 Philipp Pahl 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
"""
Data warehouse
"""

import peachbox
import peachbox.fs

import simplejson as json

class DWH(object):
    def __init__(self):
        self.fs    = peachbox.fs.AmazonDfs()
        self.init_spark()
        pass

    def init_spark(self):
        self.spark = peachbox.Spark()
    
    def shutdown(self):
        self.spark.stop()

    def query(self, model, from_utime, before_utime):
        pass
        self.fs.mart = model.mart
        sources = ','.join(self.fs.dirs_in_time_range(model, from, before))
        rdd     = self.retrieve(sources) 
        return self.load_json(rdd)

    # TODO: Handle target times properly
    def write(self, model, rdd, seconds=0):
        self.fs.mart = model.mart
        path = model.path() + '/' + str(seconds)
        self.fs.rm_fr(path)
        json_rdd = self.dump_json(rdd)
        json_rdd.saveAsTextFile(self.fs.url_prefix() + path)

    def retrieve(self, sources):
        return self.spark.context().textFile(sources)

    def load_json(self, rdd)
        return rdd.map(lambda line: json.loads(line))

    def dump_json(self, rdd):
        return rdd.map(lambda line: json.dumps(line))


