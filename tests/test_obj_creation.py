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
        self.assertTrue(ds.exists)

    def test_create_subject(self):
        from src.ResearchOS.DataObjects.subject import Subject
        sj = Subject(id = "SJ000000_000")
        self.assertTrue(sj.id == "SJ000000_000")
        self.assertTrue(sj.exists)

    def test_create_visit(self):
        from src.ResearchOS.DataObjects.visit import Visit
        vs = Visit(id = "VS000000_000")
        self.assertTrue(vs.id == "VS000000_000")
        self.assertTrue(vs.exists)

    def test_create_trial(self):
        from src.ResearchOS.DataObjects.trial import Trial
        tr = Trial(id = "TR000000_000")
        self.assertTrue(tr.id == "TR000000_000")
        self.assertTrue(tr.exists)

    def test_create_phase(self):
        from src.ResearchOS.DataObjects.phase import Phase
        ph = Phase(id = "PH000000_000")
        self.assertTrue(ph.id == "PH000000_000")    
        self.assertTrue(ph.exists)



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