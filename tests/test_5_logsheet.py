import pytest

import ResearchOS as ros

def test_logsheet_exists(db_connection):
    """Make sure that the logsheet exists in the database after the logsheet is first created."""
    lg = ros.Logsheet(id = "LG1")
    assert isinstance(lg, ros.Logsheet)
    assert lg.id == "LG1"
    assert lg.prefix == "LG"

def test_logsheet_path_fails_if_not_exist(db_connection):
    """Make sure that the path is correct."""
    lg = ros.Logsheet(id = "LG1")
    path = "path/to/logsheet.csv"
    try:
        lg.path = path
        assert False
    except ValueError:
        assert True

def test_logsheet_path(temp_logsheet_file, db_connection):
    """Make sure that the path is correct."""
    lg = ros.Logsheet(id = "LG1")
    lg.path = temp_logsheet_file
    assert lg.path == temp_logsheet_file

def test_class_column_names(db_connection):
    """Make sure that the class column names are correct."""
    lg = ros.Logsheet(id = "LG1")
    class_column_names = {
        "Subject_Codename": ros.Subject,
        "Trial_Name_Number": ros.Trial
    }
    lg.class_column_names = class_column_names
    del lg
    lg = ros.Logsheet(id = "LG1")
    assert lg.class_column_names == class_column_names

def test_logsheet_headers(logsheet_headers, db_connection):
    """Make sure that the header names are correct."""
    lg = ros.Logsheet(id = "LG1")    
    lg.headers = logsheet_headers
    del lg
    lg = ros.Logsheet(id = "LG1")
    assert lg.headers == logsheet_headers

def test_logsheet_num_header_rows(db_connection):
    """Make sure that the number of header rows is correct."""
    lg = ros.Logsheet(id = "LG1")
    num_header_rows = 3
    lg.num_header_rows = num_header_rows
    del lg
    lg = ros.Logsheet(id = "LG1")
    assert lg.num_header_rows == num_header_rows

def test_read_logsheet(temp_logsheet_file, logsheet_headers, db_connection):
    """Make sure that the logsheet can be read."""
    lg = ros.Logsheet(id = "LG1")
    lg.path = temp_logsheet_file
    ss = ros.Subset(id = "SS1")
    lg.subset_id = ss.id
    lg.headers = logsheet_headers
    lg.num_header_rows = 3
    lg.read_logsheet()
    # assert lg.logsheet_data == []