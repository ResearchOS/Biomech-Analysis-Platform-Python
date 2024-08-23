Input variables represent the data that is flowing into a process, plot, or summary. They are always connected to a Runnable node. They can be specified in a variety of ways, including hard-coded values, dynamic variables, or variables that are passed from other processes. Input variables are specified using the "inputs" key with named keys for each corresponding input variable. In the below example, there are two inputs, `input1` and `input2`:
```toml
[example_process_name]
inputs.input1 = "?"
inputs.input2 = "?"
outputs = ["output1"]
```

# Unspecified Variables
Input variables that have the value `"?"` are considered unspecified. These variables are not connected to any other variable nodes in the package. By leaving the variable unspecified, package creators can leave the variable open for the user to specify using the `bridges.toml` file. If the variable is not specified in the `bridges.toml` file, the variable remains unspecified. See the above code block for an example of unspecified variables.

# Dynamic Variables
To use the output of another function in the package as an input to a Runnable, the source Runnable's name & output variable should be specified. This is done by setting the input variable to a string in the format `"{runnable_name}.{output_variable}"`. For example:
```toml
[example_process_name]
inputs.input1 = "example_process_name1.output1"
inputs.input2 = "example_process_name2.output1"
outputs = ["output1"]
```
If there is no runnable and output variable by that name, then the string is treated as a hard-coded value.

## Data Object Name

## Data Object File Path

# Hard-Coded Variables
## Value
If an input variable is a hard-coded value, a default value can be specified. For example:
```toml
[example_process_name]
inputs.input1 = "hardcoded_value"
outputs = ["output1"]
```
Note that frequently the user will want to change hard-coded values for "what if" analyses. This can be done by specifying the value in the `bridges.toml` file, which overwrites the value here.

## Load from File
If a hardcoded variable is complex, or there are many versions of this hard-coded variable, it may be useful to load the variable from a file. JSON and TOML are supported. This can be done by specifying the input variable as a dictionary with the key `__load_file__` and the value as the file path relative to the project's root directory. For example:
```toml
[example_process_name]
inputs.input1 = {__load_file__ = "constants/input1.json"}
outputs = ["output1"]
```