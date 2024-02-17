import pytest, shutil, os

import ResearchOS as ros

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.db_connection import DBConnection
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.config import Config

# Function scoped
@pytest.fixture(scope="function")
def temp_db_file(tmp_path):
    config = Config()
    db_file = str(tmp_path / "test.db")
    config.db_file = db_file
    return db_file

# Session scoped
# @pytest.fixture(scope="session")
# def temp_db_file(tmp_path_factory):   
#     return str(tmp_path_factory.mktemp("test").joinpath("test.db"))
  
@pytest.fixture(scope="function")
def db_init(temp_db_file):
    return DBInitializer(temp_db_file)  
            
@pytest.fixture(scope="function")
def db_connection(temp_db_file, db_init):
    # TODO: This needs to not be a singleton, so I can have clean tests that don't share a database connection with a different file.
    # DBConnection._instance = None
    # pool = SQLiteConnectionPool(temp_db_file, 5)
    return True
    # SQLiteConnectionPool._instance = None
    # conn = pool.get_connection()
    # # db = DBConnectionFactory().create_db_connection(temp_db_file, singleton = True)
    # yield conn
    # pool.return_connection(conn)
    # if os.path.exists(temp_db_file):
    #     os.unlink(temp_db_file)
        # os.remove(temp_db_file)

@pytest.fixture(scope="session")
def temp_logsheet_file(tmp_path_factory):
    logsheet_path = str(tmp_path_factory.mktemp("test").joinpath("logsheet.csv"))
    shutil.copy("Spr23TWW_OA_AllSubjects_032323.csv", logsheet_path)
    return logsheet_path

# Logsheet
@pytest.fixture(scope="function")
def logsheet_headers():
    incomplete_headers = [
        ("Date", str, ros.Subject),
        ("Subject_Codename", str, ros.Subject),
        ("Cohort", str, ros.Subject),
        ("Age", int, ros.Subject),
        ("Gender", str, ros.Subject),
        ("FPs_Used", str, ros.Subject),
        ("Visit_Number", int, ros.Subject),
        ("Trial_Name_Number", str, ros.Trial),
        ("Trial_Type_Task", str, ros.Trial),
        ("Side_Of_Interest", str, ros.Trial),
        ("Perfect_Trial", int, ros.Trial),
        ("Subject_Comments", str, ros.Trial),
        ("Researcher_Comments", str, ros.Trial),
        ("Motive_Initial_Frame", int, ros.Trial),
        ("Motive_Final_Frame", int, ros.Trial)
    ]
    headers = []
    for i in range(0, 15):
        vr = ros.Variable(id = f"VR{i}")
        headers.append((incomplete_headers[i][0], incomplete_headers[i][1], incomplete_headers[i][2], vr.id))
    return headers

if __name__ == "__main__":
    pytest.main(["-v", "tests/"])