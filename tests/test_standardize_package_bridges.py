import os
import shutil
from pathlib import Path
from copy import deepcopy

import pytest
import tomli

from ResearchOS.compile import standardize_package_bridges
from ResearchOS.constants import PROJECT_NAME_KEY
from fixtures.constants import TMP_PACKAGES_PATH

def test_standardize_package_bridges(tmp_path: Path = TMP_PACKAGES_PATH):
    # Create a test project folder    
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

    shutil.copytree("tests/fixtures/packages", tmp_path)
    os.environ[PROJECT_NAME_KEY] = "package2"
    
    project_path = os.path.join(tmp_path, os.environ[PROJECT_NAME_KEY])
    assert standardize_package_bridges({}, project_path) == {}

    with open(os.path.join(project_path, "src/bridges.toml"), "rb") as f:
        bridges_toml_orig = tomli.load(f)

    bridges_toml = deepcopy(bridges_toml_orig)
    del bridges_toml['var_a']['sources']
    with pytest.raises(ValueError):
        standardize_package_bridges(bridges_toml, project_path)

    bridges_toml = deepcopy(bridges_toml_orig)
    del bridges_toml['var_a']['targets']
    with pytest.raises(ValueError):
        standardize_package_bridges(bridges_toml, project_path)

    bridges_toml = deepcopy(bridges_toml_orig)    
    
    # Empty sources and targets.
    bridges_toml_empty = {'var_a': {}}
    bridges_toml_empty['var_a'] = {
        'sources': '',
        'targets': ''
    }
    with pytest.raises(ValueError):
        standardize_package_bridges(bridges_toml_empty, project_path)

    bridges_toml_manual = {'var_a': {}}
    bridges_toml_manual['var_a'] = {
        'sources': ['package1.process1.a', 'package1.process1.b'],
        'targets': ['package1.process2.b', 'package1.process2.c']
    }
    with pytest.raises(ValueError):
        standardize_package_bridges(bridges_toml_manual, project_path)

    bridges_toml_manual = {'var_a': {}}
    bridges_toml_manual['var_a'] = {
        'sources': 'package1.process1.a',
        'targets': 'package1.process2.b'
    }
    bridges_toml_manual_standardized = {'var_a': {}}
    bridges_toml_manual_standardized['var_a'] = {
        'sources': ['package1.process1.a'],
        'targets': ['package1.process2.b']
    }

    assert standardize_package_bridges(bridges_toml_manual, project_path) == bridges_toml_manual_standardized
    assert standardize_package_bridges(bridges_toml_manual_standardized, project_path) == bridges_toml_manual_standardized

    bridges_toml_manual = {'var_a': {}}
    bridges_toml_manual['var_a'] = {
        'sources': '__logsheet__.a',
        'targets': 'package1.process2.b'
    }
    bridges_toml_manual_standardized = {'var_a': {}}
    bridges_toml_manual_standardized['var_a'] = {
        'sources': [os.environ[PROJECT_NAME_KEY] + ".logsheet.a"],
        'targets': ['package1.process2.b']
    }

    # Everything working.
    assert standardize_package_bridges(bridges_toml_manual, project_path) == bridges_toml_manual_standardized

    # Clean up
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    pytest.main(["-v", __file__])