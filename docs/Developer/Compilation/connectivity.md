To construct the Directed Acyclic Graph (DAG), the following steps are performed within the `compile_packages_to_dag()` function:

- The project name is obtained from the `pyproject.toml` file's `["project"]["name"]` key.

- The logsheet metadata is obtained from the project's `package-settings.toml` file. `["outputs"]` key is added to the logsheet, populated based on the `logsheet.headers` key from that file.

- The rest of the package's settings are obtained from the `package-settings.toml` file: `DATASET_FILE_SCHEMA`, `DATASET_SCHEMA`, `SAVE_DATA_FOLDER`, `RAW_DATA_FOLDER`.

- Get the package names from the all the folders within the `project_folder` and `packages_parent_folders` that begin with `ros-`.

!!! warning
    It is possible that `pyproject.toml` field and the folder name do not match, which could cause issues. 
    
    In the future this discovery method should use the `pyproject.toml` files so that it is more robust and explicit, and uses the `pyproject.toml` file as the single source of truth.

- Create the DAG for each individual package. The following types of nodes are added here, with connectivity-related metadata only: [Runnables](../Nodes/Runnables/index.md), [Input Variables](../Nodes/Variables/Input%20Variables/index.md), and [Output Variables](../Nodes/Variables/output_variables.md). Minimum specifications for this step in the `.toml` files are:
    - [All Runnables](../Nodes/Runnables/index.md):
        - "inputs": At least one input variable specified.
        - [Process](../Nodes/Runnables/process.md):
            - "outputs": Can be an empty list, or a list of output variables.

- Get the [bridges](../Bridges/index.md) listed for each package from the package's `bridges.toml` file. The bridges are used to connect the packages together, and are applied in the topological order of the packages.
    - This process converts [Unspecified Input Variables](../Nodes/Variables/Input%20Variables/unspecified.md) to [Dynamic Input Variables](../Nodes//Variables//Input%20Variables/dynamic.md) if a bridge exists for them. If not, they remain [Unspecified](../Nodes/Variables/Input%20Variables/unspecified.md).