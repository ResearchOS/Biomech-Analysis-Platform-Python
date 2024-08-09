from ResearchOS.constants import DATASET_KEY

def schema_to_file(dobj: str, schema: list, file_schema: list):
    """Convert a data object from the in-memory schema to the file schema.
    "schema" and "file_schema" should both include the Dataset as the first element."""
    dobj_parts = dobj.split('.')
    file_dobj = []
    for file_sch in file_schema:
        if file_sch not in schema:
            continue
        index_in_schema = schema.index(file_sch)
        if index_in_schema >= len(dobj_parts):
            continue
        file_dobj.append(dobj_parts[index_in_schema])
    return '.'.join(file_dobj)

def file_to_schema(dobj: str, schema: list, file_schema: list):
    """Convert a data object from the file schema to the in-memory schema.
    "schema" and "file_schema" should both include the Dataset as the first element."""
    dobj_parts = dobj.split('.')
    if len(dobj_parts) != len(file_schema):
        raise ValueError(f"Data object {dobj} does not match the file schema {file_schema}.")
    in_mem_dobj = []
    for sch in schema:
        if sch not in file_schema:
            continue
        index_in_file_schema = file_schema.index(sch)
        if index_in_file_schema >= len(dobj_parts):
            continue
        in_mem_dobj.append(dobj_parts[index_in_file_schema])
    return '.'.join(in_mem_dobj)