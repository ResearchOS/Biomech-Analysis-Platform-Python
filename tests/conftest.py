import pytest, os

from ResearchOS.database_init import DBInitializer


@pytest.fixture
def temp_db_file(tmp_path):   
    return str(tmp_path / "test.db")
  

@pytest.fixture
def db(temp_db_file):
    return DBInitializer(temp_db_file)    
            



