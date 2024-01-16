import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/src")
import tests
from ResearchOS.config import Config
from ResearchOS import DBInitializer

from unittest import TestCase

class TestDatabase(TestCase):

    def setup_class(self):        
        self.config = Config()
        db = DBInitializer()      

    def teardown_class(self):
        # os.remove(self.config.db_file)
        pass

    def test_db_exists(self):
        self.assertTrue(os.path.isfile(self.config.db_file))   

if __name__=="__main__":
    td = TestDatabase()
    td.setup_class()
    td.test_db_exists()
    td.teardown_class()