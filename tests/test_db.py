import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/src")
from ResearchOS.config import Config
from ResearchOS import DBInitializer


class TestDatabase:

    def setup_class(self):        
        self.config = Config()
        db = DBInitializer()

    def teardown_class(self, db_conn):
        # os.remove(self.config.db_file)
        pass

    def test_db_exists(self, db_conn):
        assert os.path.isfile(self.config.db_file)

if __name__=="__main__":
    td = TestDatabase()
    td.setup_class()
    td.test_db_exists()
    td.teardown_class()