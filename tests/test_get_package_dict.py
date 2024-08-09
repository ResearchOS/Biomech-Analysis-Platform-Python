import os
from pathlib import Path
import shutil

import pytest
import toml

from ResearchOS.compile import get_package_index_dict
from ResearchOS.constants import ALLOWED_INDEX_KEYS, PROCESS_NAME, PLOT_NAME, STATS_NAME, BRIDGES_KEY, PACKAGE_SETTINGS_KEY, SUBSET_KEY
from fixtures.constants import TMP_PACKAGES_PATH



def test_get_package_index_dict(tmp_path: Path = TMP_PACKAGES_PATH):

    # Create a temporary directory
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)    

    # 1. The package_path is nonexistent.
    nonexistent_path = tmp_path
    with pytest.raises(NotADirectoryError):
        get_package_index_dict(nonexistent_path)

    os.makedirs(tmp_path)

    # 2. The package_path is a file.
    file_path = Path(tmp_path / "file.txt")
    with pytest.raises(NotADirectoryError):
        get_package_index_dict(file_path)
    
    # 3. The package_path does not contain a pyproject.toml file.
    package_path = Path(tmp_path / "package1")
    os.makedirs(package_path)
    with pytest.raises(FileNotFoundError):
        get_package_index_dict(package_path)

    # 4. Contains pyproject.toml but missing the [tool.researchos] section.
    pyproject_path = package_path / "pyproject.toml"
    open(pyproject_path, "w").close()
    with pytest.raises(KeyError):
        get_package_index_dict(package_path)

    # 5. No "index" field in the [tool.researchos] section.
    with open(pyproject_path, "w") as f:
        f.write("[tool.researchos]\n")
    with pytest.raises(KeyError):
        get_package_index_dict(package_path)

    srcPath = package_path / "src" # By convention, everything except pyproject.toml will be in this folder.
    os.makedirs(srcPath)

    # 6. No index.toml file exists at the path pointed to by the "index" field.
    with open(pyproject_path, "w") as f:
        f.write("[tool.researchos]\nindex = \"src/index.toml\"")
    with pytest.raises(FileNotFoundError):
        get_package_index_dict(package_path)

    # 7. The index.toml file is empty (missing all keys).
    open(package_path / "src/index.toml", "w").close()
    # with pytest.warns(UserWarning):
    #     get_package_index_dict(package_path)

    # 8. A path is present but the file does not exist.
    with open(package_path / "src/index.toml", "w") as f:
        f.write(f"process='{package_path}/src/nonexistent.txt'\n")
    with pytest.raises(FileNotFoundError):
        get_package_index_dict(package_path)

    # 9. The file path exists but it is not a TOML file.
    open(package_path / "src/nonexistent.txt", "w").close()
    with pytest.raises(FileNotFoundError):
        get_package_index_dict(package_path)

    # 8. The index.toml file contains everything needed.
    expected_result = {
        PROCESS_NAME: [str(srcPath / (PROCESS_NAME + ".toml"))],
        PLOT_NAME: [str(srcPath / (PLOT_NAME + ".toml"))],
        STATS_NAME: [str(srcPath / (STATS_NAME + ".toml"))],
        BRIDGES_KEY: [str(srcPath / (BRIDGES_KEY + ".toml"))],        
        PACKAGE_SETTINGS_KEY: [str(srcPath / (PACKAGE_SETTINGS_KEY + ".toml"))],
        SUBSET_KEY: [str(srcPath / (SUBSET_KEY + ".toml"))]
    }    
    for key in ALLOWED_INDEX_KEYS:
        with open(srcPath / f"{key}.toml", "w") as f:
            f.write(f"{key}='{package_path}/src/{key}.toml'\n")
    with open(package_path / "src/index.toml", "w") as f:
        toml.dump(expected_result, f)
    assert get_package_index_dict(package_path) == expected_result

    # 9. The index.toml contains strings, not lists.
    str_inputs_dict = {key: [str(value[0])] for key, value in expected_result.items()}
    with open(package_path / "src/index.toml", "w") as f:
        toml.dump(str_inputs_dict, f)
    assert get_package_index_dict(package_path) == expected_result

    # Clean up
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    test_get_package_index_dict()