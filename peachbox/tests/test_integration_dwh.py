import unittest
import pytz
import datetime
from utils import TestHelper

import peachbox.fs
import peachbox

class TestModel(object):
    def __init__(self):
        self._path = ''
        self.mart  = ''

    # Mock
    def path(self):
        return self._path + '/3'

class TestIntegrationDWH(unittest.TestCase):
    def setUp(self):
        self.dwh = peachbox.DWH()
        self.model = TestModel()

    def tearDown(self):
        self.dwh.shutdown()
