# Introduction
I put in a command-line interface (CLI) to make it easier to use the program. The CLI is a simple text-based interface that allows you to interact with the program using commands. 

## init-project
```bash
ros init-project
```
Creates a new project in the current directory. The project will have the following structure:
```plaintext
project/
    data/
    docs/
    output/
        plots/
        stats/
    packages/
    src/
        plots/
        stats/
        process/
        research_objects/
            data_objects.py
            datasets.py
            logsheets.py
            plots.py
            processes.py
            stats.py
            subsets.py
            variables.py
        __init__.py
        main.py
    tests/
    .gitignore
    config.json
    LICENSE.md
    paths.py
    pyproject.toml
    README.md
    researchos.db
    researchos_data.db
```

## init-package
```bash
ros init-package my_package
```
Creates a new package in the `packages/` directory. The package will have the following structure:
```plaintext
project/
    packages/
        my_package/
            __init__.py
            pyproject.toml
            main.py
            LICENSE.md
            README.md
            docs/            
            tests/
            src/
                plots/
                stats/
                process/
                research_objects/
                    data_objects.py
                    datasets.py
                    logsheets.py
                    plots.py
                    processes.py
                    stats.py
                    subsets.py
                    variables.py
                __init__.py
                main.py
```

## config
```bash
ros config config_name
```
Allows the user to change some of the configuration settings for the project. The user can change the following settings:
- db_file: The name of the database file.
- data_db_file: The name of the data database file.
- data_objects_path: The path to the data objects file (?)

## db-reset
```bash
ros db-reset
```
Resets the database. This will delete all data in the database and reset the schema.

## dobjs
```bash
ros dobjs dobj_path_part1 dobj_path_part2 ...
```
Lists the data objects in the database that match the given path parts. The path parts are used to filter the data objects. For example, `ros dobjs Subject1` will list all data objects with the name `Subject1`. If not arguments, prints all data objects.

## logsheet-read
```bash
ros logsheet-read
```
Reads a logsheet file and constructs the dataset from it in the database.

## show-graph
```bash
ros show-graph
```
Shows the current data processing graph (NOT DONE).

## edit
```bash
ros edit research_object_type_name package_name
```

Opens the file for the given research object type in the given package in the default text editor. If package name is omitted, opens the file in the main project directory.

## show-pl
```bash
ros show-pl
```
Shows the current data processing pipeline.

## run
```bash
ros run pl_obj_id
```
Runs the data processing pipeline. If a pipeline object ID is given, runs the pipeline from that object. If no ID is given, runs the entire pipeline. Takes care not to re-run objects that have already been run with no changes.

## init-bridges
```bash
ros init-bridges
```
Create the bridges file for the project, pre-populated.

## get
```bash
ros get node_id vr_id
```
Get the value of the variable with the given variable ID in the given node ID. Throws an error if no `node_id` specified. Lists all VR's if no `vr_id` specified.