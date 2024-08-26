
Hard-coded variables specified by the user. If a list is provided, then the node is split into N nodes, where N is the length of the list. Each node is assigned a variable from the list. Hard-coded variables can consist of any combination of the following types:

# Hard-Coded
If an input variable is a hard-coded value, a default value can be specified. For example:
```toml
[example_process_name]
inputs.input1 = "hardcoded_value"
outputs = ["output1"]
```
Note that frequently the user will want to change hard-coded values for "what if" analyses. This can be done by specifying the value in the `bridges.toml` file, which overwrites the value here.

# Load From File
If a hardcoded variable is complex, or there are many versions of this hard-coded variable, it may be useful to load the variable from a file. JSON and TOML are supported. This can be done by specifying the input variable as a dictionary with the key `__load_file__` and the value as the file path relative to the project's root directory. For example:

Syntax is:
```toml
[example_process]
inputs.hard_coded1 = {__load_file__ = 'path/to/file'}
```

# Data Object Name
The name of the data object to be used as the variable. Specify the data object factor name.

Syntax is:
```toml
[example_process]
inputs.hard_coded1 = {__data_object__ = 'Subject'}
```

# Raw Data File Path
The path to the raw data file to be used as the variable. Specify the data object factor name.

Syntax is:
```toml
[example_process]
inputs.hard_coded1 = {__data_file__ = 'Subject'}
```

!!! note
    This page is a work in progress. The information here is incomplete and may be inaccurate.