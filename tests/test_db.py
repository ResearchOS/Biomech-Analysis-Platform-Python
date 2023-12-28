from src.ResearchOS.SQL.database_init import DBInitializer
from src.ResearchOS.config import TestConfig

import os
from unittest import TestCase

db_file = TestConfig.db_file

class TestDatabase(TestCase):

    db_file: str = db_file

    def setup_class(self):
        db = DBInitializer(db_file)      

    def teardown_class(self):
        os.remove(self.db_file)

    def test_db_exists(self):
        self.assertTrue(os.path.isfile(self.db_file))   

if __name__=="__main__":
    td = TestDatabase()
    td.setup_class()
    td.test_db_exists()
    td.teardown_class()