Describes the structure of the `bridges.toml` file, which is used to configure bridges. This file is used to connect packages together in a data processing pipeline. 

Note that even variables that already have values specified in an earlier `bridges.toml` file will be overwritten if specified here.

# Dynamic Variables
Dynamic variables should be specified using "input" format, with multiple source variables and one target variable. The below example bridge shows two source variables connecting to a target variable. This means that the downstream pipeline will be split, with one branch for each source variable.

```toml
[example_bridge_name]
sources = [
    "source_package_name1.source_process_name1.source_variable_name1",
    "source_package_name2.source_process_name2.source_variable_name2",
]

targets = [
    "target_package_name1.target_process_name1.target_variable_name1",
]
```

# Hard-Coded Variables
Hard-coded variables should also be specified with the "input" format. Note that the sources are simply hard-coded values. If a list of multiple values are provided, then the downstream pipeline will be split, with one branch for each value.
```toml
[example_hard_coded_bridge_name]
sources = [
    0, 1, 2,
]
targets = [
    "target_package_name1.target_process_name1.target_variable_name1",
]
```

If a list is intended to be the input, then encapsulate the list with additional brackets. The below example provides two versions of the same input: a list, and a scalar.
```toml
[example_hard_coded_bridge_name]
sources = [
    [0, 1, 2],
    3
]
targets = [
    "target_package_name1.target_process_name1.target_variable_name1",
]
```