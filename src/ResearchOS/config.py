# Purpose: Configuration file for the application

class GeneralConfig():
    abstract_id_len = 6
    instance_id_len = 3
    # class_info = [
    #             ["ResearchObject", "RO"],
    #             ["DataObject", "DO"],
    #             ["PipelineObject", "PO"],
    #             ["User", "US"],
    #             ["Project", "PJ"],
    #             ["Analysis", "AN"],
    #             ["ProcessGroup", "PG"],
    #             ["Process", "PR"],
    #             ["Variable", "VR",]
    #             ["Dataset", "DS"],
    #             ["Subject", "SJ"],
    #             ["Visit", "VS"],
    #             ["Trial", "TR"],
    #             ["Phase", "PH"], 
    #             ["Plot", "PL"],
    #         ]

class ProdConfig(GeneralConfig):
    db_file = 'src/ResearchOS/SQL/database.db'

class DevConfig(GeneralConfig):
    # When would I use this one?
    pass

class TestConfig(GeneralConfig):
    db_file = 'tests/test_database.db'