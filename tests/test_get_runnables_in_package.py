import os
import shutil
from pathlib import Path

import pytest
import toml

from ResearchOS.create_dag_from_toml import get_runnables_in_package
from ResearchOS.constants import RUNNABLE_TYPES, PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME
from fixtures.constants import TMP_PACKAGES_PATH

def test_get_runnables_in_package(tmp_path: Path = TMP_PACKAGES_PATH):
    
    # Create a temporary directory
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

    os.makedirs(tmp_path)

    processes_dict = {
        "process1": {
            "command": "python script1.py",
            "inputs": {"input1": "input1.toml"},
            "outputs": {"output1": "output1.toml"},
        },
        "process2": {
            "command": "python script2.py",
            "inputs": {"input2": "input2.toml"},
            "outputs": {"output2": "output2.toml"},
        },
    } 

    plots_dict = {
        "plot1": {
            "script": "plot1.py",
            "inputs": {"input1": "input1.toml"},
        },
        "plot2": {
            "script": "plot2.py",
            "inputs": {"input2": "input2.toml"},
        },
    }

    stats_dict = {
        "stats1": {
            "script": "stats1.py",
            "inputs": {"input1": "input1.toml"},
        },
        "stats2": {
            "script": "stats2.py",
            "inputs": {"input2": "input2.toml"},
        },
    }

    logsheet_dict = {
        "num_header_rows": 1,
        "headers": ["header1", "header2"],
    }
    
    package_index_dict = {
        PROCESS_NAME: ['processes.toml'],
        PLOT_NAME: ['plots.toml'],
        STATS_NAME: ['stats.toml'],
        LOGSHEET_NAME: ['logsheet.toml'],
    }

    # Assumes that the package exists, and the index is proper.
    # Testing here is focused on the .toml files listed in the index.
    processes_toml = tmp_path / "processes.toml"
    plots_toml = tmp_path / "plots.toml"
    stats_toml = tmp_path / "stats.toml"
    logsheet_toml = tmp_path / "logsheet.toml"

    # 1. The runnable .toml file is empty.
    with open(processes_toml, "w") as f:
        f.write("")
    with open(plots_toml, "w") as f:
        f.write("")
    with open(stats_toml, "w") as f:
        f.write("")
    with open(logsheet_toml, "w") as f:
        f.write("")
    empty_runnable_dict = {
        PROCESS_NAME: {},
        PLOT_NAME: {},
        STATS_NAME: {},
        LOGSHEET_NAME: {},
    }
    assert get_runnables_in_package(str(tmp_path), package_index_dict) == empty_runnable_dict

    # All TOML-legal table names are valid table names here, so no need to check that.

    # Non-runnable keys are provided in the "runnable_keys" input list.
    runnable_keys = ["bad_key"]
    with pytest.raises(ValueError):
        get_runnables_in_package(tmp_path, package_index_dict, runnable_keys)

    with open(processes_toml, "w") as f:
        toml.dump(processes_dict, f)
    with open(plots_toml, "w") as f:
        toml.dump(plots_dict, f)
    with open(stats_toml, "w") as f:
        toml.dump(stats_dict, f)
    with open(logsheet_toml, "w") as f:
        toml.dump(logsheet_dict, f)

    # Call the function with the proper inputs.
    package_runnables_dict = get_runnables_in_package(tmp_path, package_index_dict, RUNNABLE_TYPES)

    full_runnable_dict = {
        STATS_NAME: stats_dict,
        PROCESS_NAME: processes_dict,
        PLOT_NAME: plots_dict,
        LOGSHEET_NAME: logsheet_dict
    }
    # Check the output
    assert package_runnables_dict == full_runnable_dict

    # Call the function with all runnables except logsheet.
    package_runnables_dict = get_runnables_in_package(tmp_path, package_index_dict, [PROCESS_NAME, PLOT_NAME, STATS_NAME])

    full_runnable_dict_except_logsheet = {
        STATS_NAME: stats_dict,
        PROCESS_NAME: processes_dict,
        PLOT_NAME: plots_dict,
    }
    # Check the output
    assert package_runnables_dict == full_runnable_dict_except_logsheet

    # Clean up
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    pytest.main([__file__])