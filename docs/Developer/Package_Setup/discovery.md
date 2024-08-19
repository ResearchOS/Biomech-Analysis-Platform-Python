Packages are discovered within the current package's folder.

# discover_packages
## Inputs:
project_folder: The current package's root folder. Default: current directory

parent_folders: The folders that may contain packages of interest. Default: current directory.

## Outputs:
package_folders: The list of folders inside of the `project_folder` and `parent_folders` starting with `ros-`. This includes packages installed by `pip` and other package managers.

The package names are defined in each package's `pyproject.toml` file's `["project"]["name"]` tables.