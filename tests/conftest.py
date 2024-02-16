import pytest, shutil

import ResearchOS as ros

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection_factory import DBConnectionFactory

# Function scoped
@pytest.fixture
def temp_db_file(tmp_path):
    return str(tmp_path / "test.db")

# Session scoped
# @pytest.fixture(scope="session")
# def temp_db_file(tmp_path_factory):   
#     return str(tmp_path_factory.mktemp("test").joinpath("test.db"))
  
@pytest.fixture
def db_init(temp_db_file):
    return DBInitializer(temp_db_file)    
            
@pytest.fixture
def db_connection(temp_db_file, db_init):
    # TODO: This needs to not be a singleton, so I can have clean tests that don't share a database connection with a different file.
    return DBConnectionFactory().create_db_connection(temp_db_file)

@pytest.fixture(scope="session")
def temp_logsheet_file(tmp_path_factory):
    logsheet_path = str(tmp_path_factory.mktemp("test").joinpath("logsheet.csv"))
    shutil.copy("Spr23TWW_OA_AllSubjects_032323.csv", logsheet_path)
    return logsheet_path

# Logsheet
@pytest.fixture
def logsheet_headers():
    incomplete_headers = [
        ("Date", str),
        ("Subject_Codename", str),
        ("Cohort", str),
        ("Age", int),
        ("Gender", str),
        ("FPs_Used", str),
        ("Visit_Number", int),
        ("Trial_Name_Number", str),
        ("Trial_Type_Task", str),
        ("Side_Of_Interest", str),
        ("Perfect_Trial", int),
        ("Subject_Comments", str),
        ("Researcher_Comments", str),
        ("Motive_Initial_Frame", int),
        ("Motive_Final_Frame", int)
    ]
    headers = []
    for i in range(0, 15):
        vr = ros.Variable(id = f"VR{i}")
        headers.append((incomplete_headers[i][0], incomplete_headers[i][1], vr.id))
    return headers

if __name__ == "__main__":
    pytest.main(["-v", "tests/"])