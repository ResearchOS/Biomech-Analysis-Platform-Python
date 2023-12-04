# Purpose: Configuration file for the application

class GeneralConfig():
    abstract_id_len = 6
    instance_id_len = 3

class ProdConfig(GeneralConfig):
    db_file = 'SQL/database.db'

class DevConfig(GeneralConfig):
    pass

class TestConfig(GeneralConfig):
    db_file = 'tests/test_database.db'