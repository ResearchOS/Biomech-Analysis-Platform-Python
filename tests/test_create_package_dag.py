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
    # if os.path.exists(tmp_path):
    #     shutil.rmtree(tmp_path) 

    # package_name = "package1"

    # # Copy from the fixtures directory
    # shutil.copytree("tests/fixtures/packages/package1", tmp_path)

    # # Read the package's index dict.
    # package_index_dict = get_package_index_dict(tmp_path)

    # # Get the package's runnables dict
    # package_runnables_dict = get_runnables_in_package(tmp_path, package_index_dict)

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

    # Validate & standardize the package's runnables.
    # standardized_package_runnables_dict = {}
    # for key, value in package_runnables_dict.items():
    #     standardized_package_runnables_dict[key] = {}
    #     runnable_type = RunnableFactory.create(runnable_type=key)
    #     for package_name in value:
    #         is_valid, err_msg = runnable_type.validate(value[package_name], compilation_only=True)
    #         assert is_valid
    #         standardized_package_runnables_dict[key][package_name] = runnable_type.standardize(value[package_name], compilation_only=True)    

    package_name = "test_package"

    # As written, this will raise an error because an output variable within the package is specified as an input to another process, but it does not exist.
    package_dag = create_package_dag(package_runnables_dict, package_name)

    # Check the number of nodes
    assert len(package_dag.nodes) == 12

    # Check the number of edges
    assert len(package_dag.edges) == 12

    # Check the node attributes
    for node_id, node_data in package_dag.nodes(data=True):
        node = node_data["node"]
        if isinstance(node, Process):
            assert node.name.startswith(package_name + ".process")
            assert node.command.startswith("python")
        elif isinstance(node, Plot):
            assert node.name.startswith(package_name + ".plot")
            assert node.script.endswith(".py")
        elif isinstance(node, Stats):
            assert node.name.startswith(package_name + ".stats")
            assert node.script.endswith(".py")
        elif isinstance(node, OutputVariable):
            assert node.name.startswith(package_name)
            assert node.name.endswith(".toml")
        elif isinstance(node, InputVariable):
            assert node.name.startswith(package_name)
            assert node.name.endswith(".toml")
        else:
            assert False, f"Unexpected node type: {type(node)}"

    # Check the edge connections
    for source, target, edge_data in package_dag.edges(data=True):
        bridge = edge_data.get("bridge")
        assert bridge is None or bridge.startswith(package_name)

    # Clean up
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    pytest.main([__file__])