from SQL.database_init import DBInitializer
from data_object import DataObject


import os
from unittest import TestCase
from datetime import datetime

db_file = 'tests/test_database.db'

class TestDatabase(TestCase):

    db_file: str = db_file

    def setup_class(self):
        db = DBInitializer(db_file)
        DataObject.db_file = db_file

    def teardown_class(self):
        os.remove(self.db_file)

    def test_db_exists(self):
        self.assertTrue(os.path.isfile(self.db_file))

    def check_common_attrs(self, obj: DataObject):        
        self.assertTrue(obj.name == "Untitled")
        self.assertTrue(obj.description == "Description here.")
        self.assertTrue(hasattr(obj, "created_at") and isinstance(obj.created_at, datetime))
        self.assertTrue(hasattr(obj, "updated_at") and isinstance(obj.updated_at, datetime))

    def test_create_dataset(self):   
        from dataset import Dataset     
        ds1 = Dataset(uuid = "DS1")
        ds1_1 = Dataset(uuid = "DS1")
        self.assertTrue(ds1 is ds1_1) # Check that the same object is returned.
        self.assertTrue(ds1.uuid == "DS1")
        self.check_common_attrs(ds1)

    def test_create_subject(self):
        from subject import Subject
        s1 = Subject(uuid = "SB1")
        s1_1 = Subject(uuid = "SB1")
        self.assertTrue(s1 is s1_1)
        self.assertTrue(s1.uuid == "SB1")
        self.check_common_attrs(s1)

if __name__=="__main__":
    td = TestDatabase()
    td.setup_class()
    td.test_db_exists()
    td.test_create_dataset()
    td.test_create_subject()
    td.teardown_class()
        