import pytest

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection import DBConnectionSQLite

# Session scoped
@pytest.fixture(scope="session")
def temp_db_file(tmpdir_factory):   
    return tmpdir_factory.mktemp("data").join("test.db")
  
@pytest.fixture(scope="session")
def db_init(temp_db_file):
    return DBInitializer(temp_db_file)    
            
@pytest.fixture(scope="session")
def db_connection(temp_db_file):
    return DBConnectionSQLite(str(temp_db_file))


