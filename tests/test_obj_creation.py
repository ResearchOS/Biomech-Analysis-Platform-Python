import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/src")
from ResearchOS.config import Config
from ResearchOS.database_init import DBInitializer
from unittest import TestCase

import ResearchOS as ros
from ResearchOS.action import Action

class TestObjCreation(TestCase):    
    
    def setup_class(self):
        os.environ["ENV"] = "test"
        self.config = Config()
        Action._db_file = self.config.db_file        
        db = DBInitializer()        

    def teardown_class(self):
        import os
        os.remove(self.config.db_file)

    #################### USER & VARIABLE ####################
    def test_create_user(self):
        # from ResearchOS import User
        us = ros.User(id = "US000000_000")
        self.assertTrue(us.id == "US000000_000")
        self.assertTrue(us.exists)

    def test_create_variable(self):
        # from ResearchOS import Variable
        vr = ros.Variable(id = "VR000000_000")
        self.assertTrue(vr.id == "VR000000_000")
        self.assertTrue(vr.exists)

    #################### PIPELINE OBJECTS ####################
    def test_create_project(self):
        # from ResearchOS import Project
        pj = ros.Project(id = "PJ000000_040") # Random ID.
        self.assertTrue(pj.id == "PJ000000_040")
        self.assertTrue(pj.exists)

    def test_create_analysis(self):
        # from ResearchOS import Analysis
        an = ros.Analysis(id = "AN000000_000")
        self.assertTrue(an.id == "AN000000_000")
        self.assertTrue(an.exists)

    def test_create_process(self):
        # from ResearchOS import Process
        pr = ros.Process(id = "PR000000_000")
        self.assertTrue(pr.id == "PR000000_000")
        self.assertTrue(pr.exists)

    def test_create_subset(self):
        # from ResearchOS import Subset
        ss = ros.Subset(id = "SS000000_000")
        self.assertTrue(ss.id == "SS000000_000")
        self.assertTrue(ss.exists)

    #################### DATA OBJECTS ####################

    def test_create_dataset(self):   
        # from ResearchOS import Dataset          
        ds = ros.Dataset(id = "DS000000_000")
        self.assertTrue(ds.id == "DS000000_000")
        self.assertTrue(ds.exists)

    def test_create_subject(self):
        # from ResearchOS import Subject
        sj = ros.Subject(id = "SJ000000_000")
        self.assertTrue(sj.id == "SJ000000_000")
        self.assertTrue(sj.exists)

    def test_create_visit(self):
        # from ResearchOS import Visit
        vs = ros.Visit(id = "VS000000_000")
        self.assertTrue(vs.id == "VS000000_000")
        self.assertTrue(vs.exists)

    def test_create_trial(self):
        # from ResearchOS import Trial
        tr = ros.Trial(id = "TR000000_000")
        self.assertTrue(tr.id == "TR000000_000")
        self.assertTrue(tr.exists)

    def test_create_phase(self):
        # from ResearchOS import Phase
        ph = ros.Phase(id = "PH000000_000")
        self.assertTrue(ph.id == "PH000000_000")    
        self.assertTrue(ph.exists)

    #################### COPYING ####################
    def test_copy_instance_object_to_new(self):
        # from ResearchOS import Logsheet
        lg = ros.Logsheet(id = "LG000000_000")
        lg2 = lg.copy_to_new_instance()
        self.assertTrue(lg.abstract_id() == lg2.abstract_id())  
        self.assertTrue(lg.id != lg2.id)
        dict1 = lg.__dict__
        dict2 = lg2.__dict__
        del dict1["id"]
        del dict2["id"]
        self.assertTrue(dict1 == dict2)      



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