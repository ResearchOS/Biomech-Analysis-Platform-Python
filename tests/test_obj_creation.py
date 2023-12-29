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

    #################### USER & VARIABLE ####################
    def test_create_user(self):
        from src.ResearchOS.user import User
        us = User(id = "US000000_000")
        self.assertTrue(us.id == "US000000_000")
        self.assertTrue(us.exists)

    def test_create_variable(self):
        from src.ResearchOS.variable import Variable
        vr = Variable(id = "VR000000_000")
        self.assertTrue(vr.id == "VR000000_000")
        self.assertTrue(vr.exists)

    #################### PIPELINE OBJECTS ####################
    def test_create_project(self):
        from src.ResearchOS.PipelineObjects.project import Project
        pj = Project(id = "PJ000000_000")
        self.assertTrue(pj.id == "PJ000000_000")
        self.assertTrue(pj.exists)

    def test_create_analysis(self):
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        an = Analysis(id = "AN000000_000")
        self.assertTrue(an.id == "AN000000_000")
        self.assertTrue(an.exists)

    def test_create_process(self):
        from src.ResearchOS.PipelineObjects.process import Process
        pr = Process(id = "PR000000_000")
        self.assertTrue(pr.id == "PR000000_000")
        self.assertTrue(pr.exists)

    def test_create_subset(self):
        from src.ResearchOS.PipelineObjects.subset import Subset
        ss = Subset(id = "SS000000_000")
        self.assertTrue(ss.id == "SS000000_000")
        self.assertTrue(ss.exists)

    #################### DATA OBJECTS ####################

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