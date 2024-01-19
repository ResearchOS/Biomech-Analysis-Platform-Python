import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/src")
from ResearchOS.config import Config
from ResearchOS.action import Action
from ResearchOS import DBInitializer

@pytest.fixture(scope="function")
def db_conn():
    return Action.conn

class TestDatabase:

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