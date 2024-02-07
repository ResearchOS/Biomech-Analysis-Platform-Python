import pytest

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection import DBConnectionSQLite

# Function scoped
@pytest.fixture
def temp_db_file(tmp_path):   
    return str(tmp_path / "test.db")
  
@pytest.fixture
def db_init(temp_db_file):
    return DBInitializer(temp_db_file)    
            
# Session scoped
@pytest.fixture(scope="session")
def db_connection(tmpdir_factory):
    temp_db_file = tmpdir_factory.mktemp("data").join("test.db")
    return DBConnectionSQLite(str(temp_db_file))


