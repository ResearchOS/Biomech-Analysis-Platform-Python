from pathlib import Path
import os
import shutil

import pytest

from ResearchOS.create_dag_from_toml import create_package_dag, get_package_index_dict, get_runnables_in_package
from ResearchOS.custom_classes import Process, Plot, Stats, OutputVariable, InputVariable
from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME
from fixtures.constants import TMP_PACKAGES_PATH
from ResearchOS.validation_classes import RunnableFactory

def test_create_package_dag(tmp_path: Path = TMP_PACKAGES_PATH):

    # Create a temporary directory
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path) 
    os.makedirs(tmp_path)

    package_runnables_dict = {}
    package_runnables_dict[PLOT_NAME] = {
    }
    package_runnables_dict[STATS_NAME] = {
    }
    package_runnables_dict[LOGSHEET_NAME] = {
    }
    package_runnables_dict[PROCESS_NAME] = {}
    package_runnables_dict[PROCESS_NAME]['test_process1'] = {
        "path": 'tests/fixtures/packages/package1/process1.py',
        "inputs": {
            "input1": '?',
            "input2": 'test_process2.output1'
        },
        "outputs": []
    }       

    package_name = "test_package"

    # As written, this will raise an error because an output variable within the package is specified as an input to another process, but it does not exist.
    with pytest.raises(ValueError):
        package_dag = create_package_dag(package_runnables_dict, package_name)

    # Fix the error by adding the missing process and output variable.
    package_runnables_dict[PROCESS_NAME]['test_process2'] = {
        "inputs": { 'input1': '?'},
        "outputs": ['output1']
    }

    package_dag = create_package_dag(package_runnables_dict, package_name)
    assert len(package_dag.edges) == 5
    assert len(package_dag.nodes) == 6

    # Clean up
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    pytest.main([__file__])