# Directional Acyclic Graph (DAG)

The Directed Acyclic Graph (DAG) is a [NetworkX MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html) that represents the dependencies between the various tasks in a project. The DAG is created by the `ros compile` command, which reads the `.toml` files in the project and creates a graph of the tasks and their dependencies. The DAG is then used to determine the order in which the tasks should be run, and whether a task needs to be re-run based on the inputs and outputs of the tasks.

The DAG is structured such that the index of each node is a UUID from Python's `uuid` module. This allows for easy identification of nodes and edges, and for easy serialization and deserialization of the DAG. Within each node is a "node" attribute containing an instance of either a `Runnable` or a `Variable` class.

Each node of the DAG represents either:

1. A Runnable: A task that can be run. May be a Process, a Plot, or a Stats task.

2. A Variable: A node that connects to or from a Runnable, representing an input or output variable of the Runnable. Also connects to other Variables to form the dependencies between Runnables.

Only the nodes in the DAG contain metadata needed to run the tasks and pass data between them. The edges indicate connections between the nodes, but do not contain any metadata.

## Hashing
Each node can be hashed in either a "static" or "concrete" fashion, to borrow nomenclature from the OOP world. The static hash is used to uniquely identify the node in the DAG, while the concrete hash is used to uniquely identify the node for specific data objects. The [Weisfeiler Lehman Subgraph Hash algorithm from NetworkX](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.graph_hashing.weisfeiler_lehman_subgraph_hashes.html#networkx.algorithms.graph_hashing.weisfeiler_lehman_subgraph_hashes) is used to uniquely hash each node in the DAG based on its "node" attribute. Each subclass of `Runnable` and `Variable` has implemented a `__hash__` method that returns a unique hash for that node based on its metadata for `static` and `concrete` hashing.

!!! todo
    OPEN QUESTION: How do I cache the concrete DAG? I cant very well load every Variable from every data object to hash all of them every time I want to run a task. I need to cache the concrete DAG somehow.

Mirroring this concept of "static" and "concrete" hashes, the most recently ran version of the DAG itself will also be stored in two ways: a "static" version stored in the "static_dag.toml" file and a "concrete_dag.toml" file in the root of the saved data's directory. The "static" DAG is used to determine the order in which tasks should be run, while the "concrete" DAG is used to determine if a task needs to be re-run based on the data objects' hashes.

### Static DAG
A dict of dicts with three levels of nesting, where the keys to the outer dicts are the static hashes of the runnable nodes. The keys to the inner dicts are the static hashes of the runnable nodes that the outer node depends on. Finally, the keys to the innermost dicts are the variable names for the task, and the values are the static hashes of the variables for that data object.

```toml
[<runnable_static_hash>]
<variable_name> = <variable_static_hash>
```

### Concrete DAG
A dict of dicts with three levels of nesting, where the keys to the outer dicts are the data object full names. The keys to the inner dicts are the static hashes of the runnable nodes. Finally, the keys to the innermost dicts are variable names for the task, and the values are the concrete hashes of the variables for that data object. NOTE: The edges between nodes are not represented here, as that information is contained within the static hashes.

```toml
[<data_object_full_name>.<runnable_static_hash>]
<runnable_name> = <runnable_full_name>
<variable_name> = <variable_concrete_hash>
```

## Runnable Nodes
### Naming Convention
A Runnable node's full name is `package_name.task_name`. Often (but not required) the `task_name` is similar to the name of the function to be run by that task.

!!! tip
    For simplicity, a Runnable node may in certain circumstances be referred to only by its `task_name` in the `.toml` files. In that case, the `package_name` is assumed to be the package defined by the root folder's `package.toml` file.

    For example, if `root/pyproject.toml` defines package `package1`, then a task within that package may be referred to as simply `task1`, and we know it is referring to `package1.task1`.

### Metadata
Each Runnable node contains metadata that is used to run the task. This metadata is stored in the `.toml` file for the task, and also in the DAG node. The metadata includes:

- full_name: The full name of the task, as described above.
- subset: The subset of data to run the task on.
- level: The level of data object to run the task on.
- batch: The batch of data objects to run the task on.

#### Static Hash
Metadata included in the static hash:

- The full name of the task
- The subset of data to run the task on
- A dictionary of input variables, where the keys are the input names and the values are the corresponding Variable nodes' full names.
- A dictionary of output variables, where the keys are the output names and the values are the corresponding Variable nodes' full names.
- NOTE: A task's static hash is added to the record as having been successfully completed after the task has been successfully run over all data objects in the subset. If any of the above metadata changes after that, the task will be re-run.
#### Concrete Hash
Metadata included in the concrete hash:

- The static hash of the task
- A dictionary of input variables, where the keys are the input names and the values are the corresponding Variable nodes' concrete hashes.

## Variable Nodes
### Naming Convention
A Variable node's full name is `package_name.task_name.variable_name`. The `variable_name` is the name of the input or output variable of the Runnable.

!!! warning
    This naming convention does not distinguish between input and output variables. Therefore, a particular task's .toml file can not contain an input and output variable with the same name.

    If "data" is an input variable, then "data" cannot be an output variable of the same task. We suggest using more specific variable names to avoid this issue.

### Metadata
Each Variable node contains metadata that is used to pass data between tasks. This metadata is stored in the `.toml` file for the task and in the `bridges.toml` file(s), and also in the DAG node. The metadata includes:

- full_name: The full name of the variable, as described above.

#### Static Hash
Metadata included in the static hash is below. Exactly what is being hashed depends on the type of Variable (hardcoded, dynamic input, data object attribute, output, etc.):

- For **hardcoded** Variables: The variable value
- For **dynamic input** Variables: The Variable's full name
- For **data object attributes**: The dict defining which attribute of the data object to use
- For **paths**: The contents of the file at the path. If that file does not exist or was not found, then the hash is the hash of an empty string ""
    - "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
- For **output** Variables: The Variable's full name

#### Concrete Hash
Metadata included in the concrete hash is below. This is the hash used to name the variable uniquely in the Parquet files, and to determine if the data has changed since the last time the task was run. Exactly what is being hashed depends on the type of Variable (hardcoded, dynamic input, data object attribute, output, etc.):

- For **hardcoded** Variables: The variable value
- For **dynamic input** Variables: The input data value
- For **data object attributes**: The value of the data object's attribute
- For **paths**: The contents of the file at the path. If that file does not exist or was not found, then the hash is the hash of an empty string ""
    - "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
- For **output** Variables: The output data value