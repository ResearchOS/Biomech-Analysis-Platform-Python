Nodes must be uniquely identifiable to load and save them to disk reliably. However, the polyfurcation of the DAG can lead to multiple nodes with the same name. To address this, each node is assigned a unique identifier (Python's UUID4). This identifier is a string that is unique across all nodes in the project, but only for a singular run of the program (because Python's UUID functions are not idempotent). Thus, the node is not identified by its name or UUID, but rather by the SHA 256 hash of the entire DAG that preceded (are ancestors of) the particular node.

The hard-coded Input Variable nodes need to be resolved to the particular Data Objects in question before being hashable. For example, if the node is a DataObjectName node, its value may be specified by the user as "Subject". For each Subject, the node's value will be resolved to a particular Data Object name.

Hashing the graph requires specific attributes for Runnable and Variable nodes. 

The following attributes of a Runnable node are used to compute the hash of a DAG:
1. node name
2. factor name
3. batch name (eventually this should change to be the dict of data objects in the batch, because a batch_name could stay the same or change without affecting the data objects or computations)
4. language

The following attributes of a Variable node are used to compute the hash of a DAG:
1. node name
2. value
3. slices

For Runnables and Variables, the relevant attributes are put into a dict, and the dict is json-serialized. Once all nodes are in a hashable form, the hash is computed using [NetworkX's Weisfeler Lehman graph hash](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.graph_hashing.weisfeiler_lehman_graph_hash.html#networkx.algorithms.graph_hashing.weisfeiler_lehman_graph_hash) algorithm. node_attr is set to `serialized` because that is the attribute that contains the hashable form of the node. 

For each node, the hash of its ancestor graph is computed. This hash is stored in the node's `hash_value` attribute. This hash is used to uniquely identify the node.