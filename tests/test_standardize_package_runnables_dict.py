import os
import shutil
from pathlib import Path

import pytest

from ResearchOS.create_dag_from_toml import standardize_package_runnables_dict
from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME

def test_standardize_process_dict():

    # Raise error if package_folder is not absolute.
    tmp_package_folder = "tests/fixtures/packages/package1"
    with pytest.raises(ValueError):
        standardize_package_runnables_dict({}, tmp_package_folder)

    # Input to this test function is one package's runnable dict.
    runnable_dict = {}
    runnable_dict[PROCESS_NAME] = {}    
    runnable_dict[PROCESS_NAME]['process1'] = {}

    standardized_runnable_dict = runnable_dict.copy()

    package_folder = "/Users/mitchelltillman/Desktop/Work/Stevens_PhD/Non_Research_Projects/ResearchOS_Python/tests/fixtures/packages/package1"

    # Empty dictionary
    assert standardize_package_runnables_dict(runnable_dict, package_folder) == standardized_runnable_dict

    # Add minimal fields: path and one unspecified input
    runnable_dict[PROCESS_NAME]['process1']['path']  = 'path/to/process1'
    runnable_dict[PROCESS_NAME]['process1']['inputs'] = ''   

    # Missing the outputs field.
    with pytest.raises(ValueError):
        standardize_package_runnables_dict(runnable_dict, package_folder)

    runnable_dict[PROCESS_NAME]['process1']['outputs'] = {'output1': '?'}

    # Inputs and outputs are strings not dicts.
    with pytest.raises(ValueError):
        standardize_package_runnables_dict(runnable_dict, package_folder)

    runnable_dict[PROCESS_NAME]['process1']['inputs'] = {
        'input1': '?'
    }  

    # Now only outputs is wrong (dict instead of list)
    with pytest.raises(ValueError):
        standardize_package_runnables_dict(runnable_dict, package_folder)

    # Set outputs to an empty string.
    runnable_dict[PROCESS_NAME]['process1']['outputs'] = ''
    standardized_runnable_dict = runnable_dict.copy()
    standardized_runnable_dict[PROCESS_NAME]['process1']['outputs'] = []
    assert standardize_package_runnables_dict(runnable_dict, package_folder) == standardized_runnable_dict

    # Set outputs to a string
    runnable_dict[PROCESS_NAME]['process1']['outputs'] = 'output1'    
    standardized_runnable_dict = runnable_dict.copy()
    standardized_runnable_dict[PROCESS_NAME]['process1']['outputs'] = ['output1']
    assert standardize_package_runnables_dict(runnable_dict, package_folder, compilation_only = True) == standardized_runnable_dict

    # Run with outputs as a list.
    runnable_dict = standardized_runnable_dict.copy()
    assert standardize_package_runnables_dict(runnable_dict, package_folder) == standardized_runnable_dict

    

if __name__=="__main__":
    pytest.main(['-v', __file__])