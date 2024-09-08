import os
import shutil
from pathlib import Path

import pytest


from ResearchOS.create_dag_from_toml import discover_packages
from fixtures.constants import PACKAGES_PREFIX, TMP_PACKAGES_PATH

def test_discover_packages(tmp_path: Path = TMP_PACKAGES_PATH):    
    tmp_path_str = str(tmp_path)
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)        
    os.makedirs(tmp_path)      

    # Test case 1: Project folder only
    package_folder = os.sep.join([os.getcwd(), "tests", "fixtures", "tmp_packages"])
    assert discover_packages(tmp_path_str) == [package_folder]

    # Test case 2: Valid package folders
    # Create a temporary directory    
    temp_dir = tmp_path
    package_folders = [
        PACKAGES_PREFIX + "package1",
        PACKAGES_PREFIX + "package2",
        PACKAGES_PREFIX + "package3"
    ]
    expected_result = [package_folder]
    for package_folder in package_folders:
        full_path = str(temp_dir / package_folder)
        expected_result.append(full_path)
        if not os.path.exists(full_path):
            os.makedirs(full_path)    
    
    # Provide a str
    assert set(discover_packages(tmp_path_str)) == set(expected_result) # Order does not matter.

    # Provide a list of length 1
    assert set(discover_packages(tmp_path_str)) == set(expected_result) # Order does not matter.

    # Test case 3: A folder is present that contains the prefix but does not match it exactly
    package_folders = [
        'r' + PACKAGES_PREFIX + "package1"        
    ]
    for package_folder in package_folders:
        full_path = str(temp_dir / package_folder)
        if not os.path.exists(full_path):
            os.makedirs(full_path)
    assert set(discover_packages(tmp_path_str)) == set(expected_result) # Order does not matter.

    # Test case 4: Non-existent project folder
    nonexistent_folder = "nonexistent"
    with pytest.raises(FileNotFoundError):
        discover_packages(nonexistent_folder)

    # Test case 5: Non-existent package folders
    with pytest.raises(FileNotFoundError):
        discover_packages(nonexistent_folder, nonexistent_folder) 

    shutil.rmtree(tmp_path)
    

if __name__ == "__main__":    
    test_discover_packages()

    # # Test case 2: Empty package folders
    # package_folders = []
    # with pytest.raises(ValueError):
    #     discover_packages(package_folders)

    # # Test case 3: Non-existent package folders
    # package_folders = [
    #     "/path/to/nonexistent1",
    #     "/path/to/nonexistent2"
    # ]
    # expected_result = []
    # assert discover_packages(package_folders) == expected_result

    # # Test case 4: Mixed valid and non-existent package folders
    # package_folders = [
    #     "/path/to/package1",
    #     "/path/to/nonexistent1",
    #     "/path/to/package2",
    #     "/path/to/nonexistent2",
    #     "/path/to/package3"
    # ]
    # expected_result = [
    #     "/path/to/package1",
    #     "/path/to/package2",
    #     "/path/to/package3"
    # ]
    # assert discover_packages(package_folders) == expected_result

    # # Test case 5: Package folders with no packages
    # package_folders = [
    #     "/path/to/folder1",
    #     "/path/to/folder2",
    #     "/path/to/folder3"
    # ]
    # expected_result = []
    # assert discover_packages(package_folders) == expected_result

    # # Test case 6: Package folders with subfolders
    # package_folders = [
    #     "/path/to/folder1",
    #     "/path/to/folder2",
    #     "/path/to/folder3"
    # ]
    # os.makedirs("/path/to/folder1/ros-package1")
    # os.makedirs("/path/to/folder2/ros-package2")
    # os.makedirs("/path/to/folder3/ros-package3")
    # expected_result = [
    #     "/path/to/folder1/ros-package1",
    #     "/path/to/folder2/ros-package2",
    #     "/path/to/folder3/ros-package3"
    # ]
    # assert discover_packages(package_folders) == expected_result

    # # Clean up created folders
    # os.rmdir("/path/to/folder1/ros-package1")
    # os.rmdir("/path/to/folder2/ros-package2")
    # os.rmdir("/path/to/folder3/ros-package3")
    # os.rmdir("/path/to/folder1")
    # os.rmdir("/path/to/folder2")
    # os.rmdir("/path/to/folder3")