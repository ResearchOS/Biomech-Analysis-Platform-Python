Output variables represent the data that is being output from a process node. In the DAG, it connects to the subsequent input variables.

Output variables are specified as a list of variable names in the `outputs` key of the process node in the .toml file. Because named output variables are typically not supported, the order of the variable names listed must match the order of the variables returned by the processing function. The names do not need to match the name of the variables returned, but the order must match. See below for an example:
```toml
[example_process_name]
inputs.input1 = "input1"
outputs = ["output1", "output2"]
```
This .toml file entry indicates that the example process returns two outputs, `output1` and `output2`, in that order.