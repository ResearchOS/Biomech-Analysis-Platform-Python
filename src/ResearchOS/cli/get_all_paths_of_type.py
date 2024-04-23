import os
import importlib

from ResearchOS.tomlhandler import TOMLHandler

def import_objects_of_type(ttype: type, toml_path: str = None) -> list:
    """Get all of the files for a specific ResearchObject type."""

    if toml_path is None:
        toml_path = "pyproject.toml"

    project_toml_handler = TOMLHandler(toml_path)
    search_str = "tool.researchos.paths.research_objects"
    search_str += "." + ttype.__name__
    paths = project_toml_handler.get(search_str)
    if not isinstance(paths, list):
        paths = [paths]

    success = False
    for path in paths:
        try:
            path = project_toml_handler.make_abs_path(path)
            module_path = project_toml_handler.file_path_to_module(path)
            mod = importlib.import_module(module_path)
            success = True
        except Exception as e:
            print(f'Could not import {path} because "{e}"')

    if not success:
        raise ImportError(f"Could not import any of the paths {paths}")
    
    objs_list = []
    for name in dir(mod):
        obj = getattr(mod, name)
        if isinstance(obj, ttype):
            objs_list.append(obj)

    return objs_list, [mod]

    