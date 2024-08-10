import os
import shutil
from pathlib import Path

import pytest
import tomli

from ResearchOS.compile import get_package_bridges
from fixtures.constants import TMP_PACKAGES_PATH

def test_get_package_bridges(tmp_path: Path = TMP_PACKAGES_PATH):
    # Create a test project folder    
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

    shutil.copytree("tests/fixtures/packages", tmp_path)
    
    with pytest.raises(ValueError):
        get_package_bridges("")

    assert get_package_bridges(tmp_path) == {} # No second argument provided.

    rel_bridge_path = "src/bridges.toml"
    project_path = os.path.join(tmp_path, "package2")
    bridge_path = os.path.join(project_path, rel_bridge_path)

    with pytest.raises(ValueError):
        get_package_bridges(project_path, bridge_path)

    # Nonexistent bridges.toml file.
    with pytest.raises(FileNotFoundError):
        get_package_bridges(project_path, "nonexistent.toml")

    # Everything working.
    with open(bridge_path, "rb") as f:
        bridge_toml = tomli.load(f)
    assert get_package_bridges(project_path, rel_bridge_path) == bridge_toml

    # For now, bridges.toml in index is limited to one file.
    bridge_paths = [os.path.join(project_path, "src/bridges.toml"), os.path.join(project_path, "src/bridges2.toml")]
    with pytest.raises(ValueError):
        get_package_bridges(project_path, bridge_paths)
    
    # Clean up
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    pytest.main(["-v", __file__])