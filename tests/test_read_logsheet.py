import os

import pytest

from ResearchOS.read_logsheet import read_logsheet

def test_read_logsheet():

    project_folder = os.sep.join([os.getcwd(), "tests", "fixtures", "packages", "package1"])
    logsheet_toml_path = os.sep.join([project_folder, "src", "logsheet.toml"])
    read_logsheet(project_folder, logsheet_toml_path=logsheet_toml_path)

if __name__ == "__main__":
    pytest.main([__file__])