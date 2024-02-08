import pytest

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection import DBConnectionSQLite

# Function scoped
def temp_db_file_function(tmp_path):
    return str(tmp_path / "test.db")

# Session scoped
@pytest.fixture(scope="session")
def temp_db_file_session(tmp_path_factory):   
    return str(tmp_path_factory.mktemp("test").joinpath("test.db"))
  
@pytest.fixture(scope="session")
def db_init_session(temp_db_file_session):
    return DBInitializer(temp_db_file_session)    
            
@pytest.fixture(scope="session")
def db_connection_session(temp_db_file_session):
    return DBConnectionSQLite(temp_db_file_session)

@pytest.fixture(scope="session")
def temp_logsheet_file(tmp_path_factory):
    logsheet_path = str(tmp_path_factory.mktemp("test").joinpath("test_logsheet.csv"))
    with open(logsheet_path, "w") as file:
        pass
    return logsheet_path

if __name__ == "__main__":
    pytest.main(["-v", "tests/"])