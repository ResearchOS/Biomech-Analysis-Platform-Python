Overview of the process of running the functions in the DAG.

!!! note
    This page is a work in progress. The information here is incomplete and may be inaccurate.

After the DAG is compiled, the following steps are taken to run the specified pipeline:
1. Sort the Runnable nodes in topological order. Special consideration is taken to ensure that the non-dynamic or unspecified variables do not adversely affect the node order.

2. Check whether the MATLAB engine needs to be loaded. If so, load it.

3. For each Runnable node in the topologically ordered list, convert the attributes that affect the variables' hash to a data structure that can be hashed such as a frozen dict, and store it in the same index in a tuple.
- Those attributes include inputs, outputs, runnable level, batch, function name, language

4. For each node in the topologically ordered list, run the node.