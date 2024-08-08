Here are all of the columns in the Pipeline table:
1. edge_id (primary key)
2. action_id_num (int)
    - always present.
3. parent_ro_id (text, foreign key to research_object table)
    - target node. If None, source_pr_id MUST be not None, and this is a disconnected output (therefore not an edge).
4. vr_name_in_code (text)
    -Never None. If this is an edge, this is the name of the Input's variable in the code. If not an edge, then corresponding Input or Output.
5. source_pr_id
    - if None, means either: 1. this is a disconnected input (therefore not an edge), OR it's a hard-coded value (vr_id is None).
6. vr_id
    - if None (and vr_name_in_code is not None), means that this is hard-coded.
7. hard_coded_value (text/None)
    - if not None, **CONTROLS** that this is hard-coded.
8. order_num (int)
    - order of the source_pr_id in the parent_ro's input Process list. Split by is_lookup True vs. False.
9. is_lookup (int) 0 or 1
    - 0: not a lookup, search for the value in the parent_ro's input Process list for this node.
    - 1: lookup, search for the data object ID from this node's lookup_pr_id. Then in that node, use the main VR & PR's to find the value.
10. show (int) 0 or 1
    - 0: do not show in the pipeline bridges
    - 1: show in the pipeline bridges
11. is_active (int) 0 or 1