from src.ResearchOS.config import TestConfig
from src.ResearchOS.SQL.database_init import DBInitializer
from unittest import TestCase

db_file = TestConfig.db_file

class TestObjCreation(TestCase):

    db_file: str = db_file
    
    def setup_class(self):
        db = DBInitializer(db_file)

    def teardown_class(self):
        import os
        os.remove(self.db_file)

    def test_create_dataset(self):   
        from src.ResearchOS.DataObjects.dataset import Dataset          
        ds = Dataset(id = "DS000000_000")
        self.assertTrue(ds.id == "DS000000_000")

    # def test_create_subject(self):
    #     from src.ResearchOS.DataObjects.subject import Subject
    #     uuid1 = _create_uuid("subjects")
    #     s1 = Subject()
    #     s1_1 = Subject(s1.uuid)
    #     s1_2 = Subject(uuid = s1.uuid)
    #     self.assertTrue(s1 is s1_1 and s1 is s1_2)        
    #     self.check_common_attrs(s1)

    # def test_create_visit(self):
    #     from src.ResearchOS.DataObjects.visit import Visit
    #     v1 = Visit(uuid = "VT1")
    #     v1_1 = Visit(uuid = "VT1")
    #     self.assertTrue(v1 is v1_1)
    #     self.assertTrue(v1.uuid == "V1")
    #     self.check_common_attrs(v1)

    # def test_create_trial(self):
    #     from src.ResearchOS.DataObjects.trial import Trial
    #     t1 = Trial(uuid = "TR1")
    #     t1_1 = Trial(uuid = "TR1")
    #     self.assertTrue(t1 is t1_1)
    #     self.assertTrue(t1.uuid == "T1")
    #     self.check_common_attrs(t1)

    # def test_create_phase(self):
    #     from src.ResearchOS.DataObjects.phase import Phase
    #     t1 = Trial(uuid = "TR1")
    #     p1 = Phase(uuid = "PH1", trial = t1)
    #     p1_1 = Phase(uuid = "PH1")
    #     self.assertTrue(p1 is p1_1)
    #     self.assertTrue(p1.uuid == "P1")
    #     self.check_common_attrs(p1)

    # def test_create_variable(self):
    #     from src.ResearchOS.variable import Variable
    #     v1 = Variable(uuid = "VR1")
    #     v1_1 = Variable(uuid = "VR1")
    #     self.assertTrue(v1 is v1_1)
    #     self.assertTrue(v1.uuid == "V1")
    #     self.check_common_attrs(v1)

if __name__=="__main__":
    toc = TestObjCreation()
    toc.setup_class()
    toc.test_create_dataset()
    toc.test_create_subject()
    toc.test_create_visit()
    toc.test_create_phase()
    toc.test_create_trial()
    toc.test_create_variable()
    toc.teardown_class()