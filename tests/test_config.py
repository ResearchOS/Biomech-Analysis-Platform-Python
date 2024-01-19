import sys, os
os.environ["ENV"] = "test"
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/src")
from ResearchOS.config import Config
from ResearchOS.database_init import DBInitializer

class TestDatabase:

    def setup_class(self):    
        db = DBInitializer()    
        # self.config = Config()        

    def teardown_class(self):
        # os.remove(self.config.db_file)
        pass

    def test_create_config(self):
        config = Config()
        self.assertTrue(os.environ.get("ENV") == "test")
        self.assertTrue(os.path.isfile(config.config_file))


if __name__=="__main__":
    td = TestDatabase()
    td.setup_class()
    td.test_create_config()
    td.teardown_class()