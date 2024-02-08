import pytest

import ResearchOS as ros

def test_logsheet_exists():
    """Make sure that the logsheet exists in the database after the logsheet is first created."""
    lg = ros.Logsheet(id = "LG1")
    assert isinstance(lg, ros.Logsheet)
    assert lg.id == "LG1"
    assert lg.prefix == "LG"

def test_logsheet_path_fails_if_not_exist():
    """Make sure that the path is correct."""
    lg = ros.Logsheet(id = "LG1")
    path = "path/to/logsheet.csv"
    try:
        lg.path = path
        assert False
    except ValueError:
        assert True

def test_logsheet_path(temp_logsheet_file):
    """Make sure that the path is correct."""
    lg = ros.Logsheet(id = "LG1")
    lg.path = temp_logsheet_file
    assert lg.path == temp_logsheet_file

def test_class_column_names():
    """Make sure that the class column names are correct."""
    lg = ros.Logsheet(id = "LG1")
    class_column_names = {
        "Subject Codename": ros.Subject,
        "Trial": ros.Trial
    }
    lg.class_column_names = class_column_names
    del lg
    lg = ros.Logsheet(id = "LG1")
    assert lg.class_column_names == class_column_names

def test_logsheet_headers():
    """Make sure that the header names are correct."""
    lg = ros.Logsheet(id = "LG1")
    vr1 = ros.Variable(id = "VR1")
    vr2 = ros.Variable(id = "VR2")
    headers = [
        ("Subject Codename", ros.Subject, vr1.id),
        ("Trial", ros.Trial, vr2.id)
    ]
    lg.headers = headers
    del lg
    lg = ros.Logsheet(id = "LG1")
    assert lg.headers == headers

def test_read_logsheet(temp_logsheet_file):
    """Make sure that the logsheet can be read."""
    lg = ros.Logsheet(id = "LG1")
    lg.path = temp_logsheet_file
    vr1 = ros.Variable(id = "VR1")
    vr2 = ros.Variable(id = "VR2")
    headers = [
        ("Subject Codename", ros.Subject, vr1.id),
        ("Trial", ros.Trial, vr2.id)
    ]
    ss = ros.Subset(id = "SS1")
    lg.subset_id = ss.id
    lg.headers = headers
    lg.num_header_rows = 3
    lg.read_logsheet()
    assert lg.logsheet_data == []