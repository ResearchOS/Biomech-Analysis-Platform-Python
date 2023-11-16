from SQL.database_init import DBInitializer
from objects.data_object import DataObject


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
        from objects.dataset import Dataset     
        ds1 = Dataset(uuid = "DS1")
        ds1_1 = Dataset(uuid = "DS1")
        self.assertTrue(ds1 is ds1_1) # Check that the same object is returned.
        self.assertTrue(ds1.uuid == "DS1")
        self.check_common_attrs(ds1)

    def test_create_subject(self):
        from objects.subject import Subject
        s1 = Subject(uuid = "SB1")
        s1_1 = Subject(uuid = "SB1")
        self.assertTrue(s1 is s1_1)
        self.assertTrue(s1.uuid == "SB1")
        self.check_common_attrs(s1)

    def test_create_visit(self):
        from objects.visit import Visit
        v1 = Visit(uuid = "VT1")
        v1_1 = Visit(uuid = "VT1")
        self.assertTrue(v1 is v1_1)
        self.assertTrue(v1.uuid == "V1")
        self.check_common_attrs(v1)

    def test_create_trial(self):
        from objects.trial import Trial
        t1 = Trial(uuid = "TR1")
        t1_1 = Trial(uuid = "TR1")
        self.assertTrue(t1 is t1_1)
        self.assertTrue(t1.uuid == "T1")
        self.check_common_attrs(t1)

    def test_create_phase(self):
        from objects.phase import Phase
        from objects.trial import Trial
        t1 = Trial(uuid = "TR1")
        p1 = Phase(uuid = "PH1", trial = t1)
        p1_1 = Phase(uuid = "PH1")
        self.assertTrue(p1 is p1_1)
        self.assertTrue(p1.uuid == "P1")
        self.check_common_attrs(p1)

    def test_create_variable(self):
        from objects.variable import Variable
        v1 = Variable(uuid = "VR1")
        v1_1 = Variable(uuid = "VR1")
        self.assertTrue(v1 is v1_1)
        self.assertTrue(v1.uuid == "V1")
        self.check_common_attrs(v1)

    def test_create_subvariable(self):
        from objects.subvariable import Subvariable
        sv1 = Subvariable(uuid = "SV1")
        sv1_1 = Subvariable(uuid = "SV1")
        self.assertTrue(sv1 is sv1_1)
        self.assertTrue(sv1.uuid == "SV1")
        self.check_common_attrs(sv1)

if __name__=="__main__":
    td = TestDatabase()
    td.setup_class()
    td.test_db_exists()
    td.test_create_dataset()
    td.test_create_subject()
    td.test_create_visit()
    td.test_create_phase()
    td.test_create_trial()
    td.test_create_variable()
    td.test_create_subvariable()
    td.teardown_class()
        