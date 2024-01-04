# DiGraph

The [NetworkX DiGraph (directional graph)](https://networkx.org/documentation/stable/reference/) is the data structure that organizes the relationships between all of the different research objects. Just like the objects themselves, the Research Object DiGraph models the relationships between all research objects **across all projects**. This is especially useful if there are objects that are common to multiple projects - they can be reused! And updates to the object in one project can propagate to other projects, if desired.

The Research Object DiGraph consists of both Data Objects and Pipeline Objects - therefore it can become quite large and cumbersome to work with. For example if there are 10 Trial objects each referencing 10 Variable objects, this can quickly become quite large. Therefore, Data Object DiGraphs and Pipeline Object DiGraphs can be created separately by using ``data_objects = True`` and ``pipeline_objects = True`` keyword arguments.

Sub-DiGraphs can also be created by specifying the top level node. For example, to work with just one project's DiGraph, use the ``parent_node = {research_object_id}`` keyword argument in the constructor, where ``{research_object_id}`` is the project object's ID.

Note that when adding objects to the DiGraph, they must exist *before* being added to the DiGraph! In the future the ability to create objects by adding them to the DiGraph may be added, but for now object creation and addition to the DiGraph are two entirely separate steps.