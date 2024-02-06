# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/src")
# os.environ["ENV"] = "test"
# from ResearchOS.config import Config
# from ResearchOS.database_init import DBInitializer

# import ResearchOS as ros
# from ResearchOS.action import Action


# class TestObjCreation:    
    
#     def setup_class(self):
#         os.environ["ENV"] = "test"
#         self.config = Config()
#         Action._db_file = self.config.db_file        
#         db = DBInitializer()
#         return db

#     def teardown_class(self):
#         import os
#         os.remove(self.config.db_file)

#     #################### USER & VARIABLE ####################
#     def test_create_user(self, db_conn):
#         us = ros.User(id = "US000000_000")        
        
#         us.id == "US000000_000"
#         us.exists == True

#     def test_create_variable(self, db_conn):        
#         vr = ros.Variable(id = "VR000000_000")
#         vr.id == "VR000000_000"
#         vr.exists == True

#     #################### PIPELINE OBJECTS ####################
#     def test_create_project(self, db_conn):
#         us = ros.User(id = "US000000_000")        
#         pj = ros.Project(id = "PJ000000_040", parent = us) # Random ID.
#         us.current_project_id == pj.id
#         pj.id == "PJ000000_040"
#         pj.exists == True

#     def test_create_analysis(self, db_conn):
#         us = ros.User(id = "US000000_000")        
#         pj = ros.Project(id = "PJ000000_040", parent = us) # Random ID.
#         an = ros.Analysis(id = "AN000000_000", parent = pj)
#         pj.current_analysis_id == an.id
#         us.current_project_id == pj.id
#         an.id == "AN000000_000"
#         an.exists == True

#     def test_create_process(self, db_conn):   
#         us = ros.User(id = "US000000_000")        
#         pj = ros.Project(id = "PJ000000_040", parent = us) # Random ID.
#         an = ros.Analysis(id = "AN000000_000", parent = pj)     
#         pr = ros.Process(id = "PR000000_000", parent = an)
#         pj.current_analysis_id == an.id
#         us.current_project_id == pj.id
#         pr.id == "PR000000_000"
#         pr.exists == True

#     def test_create_subset(self, db_conn):        
#         ss = ros.Subset(id = "SS000000_000")
#         ss.id == "SS000000_000"
#         ss.exists == True

#     #################### DATA OBJECTS ####################

#     def test_create_dataset(self, db_conn):                   
#         ds = ros.Dataset(id = "DS000000_000")
#         ds.id == "DS000000_000"
#         ds.exists == True

#     def test_create_subject(self, db_conn):        
#         sj = ros.Subject(id = "SJ000000_000")
#         sj.id == "SJ000000_000"
#         sj.exists == True

#     def test_create_visit(self, db_conn):        
#         vs = ros.Visit(id = "VS000000_000")
#         vs.id == "VS000000_000"
#         vs.exists == True

#     def test_create_trial(self, db_conn):        
#         tr = ros.Trial(id = "TR000000_000")
#         tr.id == "TR000000_000"
#         tr.exists == True

#     def test_create_phase(self, db_conn):        
#         ph = ros.Phase(id = "PH000000_000")
#         ph.id == "PH000000_000"
#         ph.exists == True

#     #################### COPYING ####################
#     def test_copy_instance_object_to_new(self):
#         # from ResearchOS import Logsheet
#         lg = ros.Logsheet(id = "LG000000_000")
#         lg2 = lg.copy_to_new_instance()
#         lg.abstract_id() == lg2.abstract_id()
#         lg.id != lg2.id
#         dict1 = lg.__dict__
#         dict2 = lg2.__dict__
#         del dict1["id"]
#         del dict2["id"]
#         dict1 == dict2



# if __name__=="__main__":
#     toc = TestObjCreation()
#     db = toc.setup_class()
#     conn = db._conn
#     # toc.test_create_user(conn)
#     toc.test_create_project(conn)
#     toc.test_create_dataset(conn)
#     toc.test_create_subject(conn)
#     toc.test_create_visit(conn)
#     toc.test_create_phase(conn)
#     toc.test_create_trial(conn)
#     toc.test_create_variable(conn)
#     toc.teardown_class()