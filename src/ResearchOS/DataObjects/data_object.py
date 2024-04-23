"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from typing import Any, TYPE_CHECKING, Union
import pickle
import os
from hashlib import sha256
import json
import copy

import numpy as np
import toml

if TYPE_CHECKING:    
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]
    
from ResearchOS.variable import Variable    
from ResearchOS.research_object import ResearchObject
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.var_converter import convert_var
from ResearchOS.tomlhandler import TOMLHandler

all_default_attrs = {}

computer_specific_attr_names = []

class DataObject(ResearchObject):
    """The parent class for all data objects. Data objects represent some form of data storage, and approximately map to statistical factors."""    

    def get(self, vr: "Variable", action: Action, process: "Process" = [], node_lineage: list = None, lookup_vr: "Variable" = None, lookup_pr: list = [], vr_name_in_code: str = None, parent_ro: "ResearchObject" = None) -> Any:
        """Load the value of a VR from the database for this data object.

        Args:
            vr (Variable): ResearchOS Variable object to load the value of.
            process (Process): ResearchOS Process object that this data object is part of.
            action (Action): The Action that this is part of.

        Returns:
            Any: The value of the VR for this data object.
        """
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        from ResearchOS.DataObjects.dataset import Dataset

        if action is None:
            action = Action(name = "get_vr_value")
        
        vr = Variable(id = vr, action=action) if (isinstance(vr, str) and vr.startswith(Variable.prefix)) else vr        
        process = [Process(id=pr, action=action) if isinstance(pr, str) and pr.startswith(Process.prefix)
            else Logsheet(id=pr, action=action) if isinstance(pr, str) and pr.startswith(Logsheet.prefix)
            else pr  # Pass other types or unmatched strings unchanged.
            for pr in process
        ] if process else []

        lookup_vr = Variable(id = lookup_vr, action=action) if (isinstance(lookup_vr, str) and lookup_vr.startswith(Variable.prefix)) else lookup_vr
        lookup_pr = [Process(id=lookup_pr, action=action) if isinstance(lookup_pr, str) and lookup_pr.startswith(Process.prefix)
            else Logsheet(id=lookup_pr, action=action) if isinstance(lookup_pr, str) and lookup_pr.startswith(Logsheet.prefix)
            else lookup_pr  # Pass other types or unmatched strings unchanged.
            for lookup_pr in lookup_pr
        ] if lookup_pr else []

        if isinstance(vr, dict):
            subclasses = DataObject.__subclasses__()
            prefix = [key for key in vr.keys()][0]
            cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix == prefix]
            if cls:
                cls = cls[0]
            attr = [value for value in vr.values()][0]
            if cls:
                node = [node for node in node_lineage if node.prefix==cls.prefix][0]
                return getattr(node, attr)
            return vr # Hard-coded value.
        
        if isinstance(vr, Variable) and vr.hard_coded_value is not None:
            return vr.hard_coded_value        
        
        # This is either hard-coded, or it's the import file VR.
        if parent_ro and hasattr(parent_ro, "import_file_vr_name") and vr_name_in_code == parent_ro.import_file_vr_name:
            dataset_id = self._get_dataset_id()
            dataset = Dataset(id = dataset_id, action=action)
            data_path = dataset.dataset_path
            for node in reversed(node_lineage):
                prefix = node.id[0:2]
                in_file_schema = any([prefix==cls.prefix for cls in dataset.file_schema])
                if in_file_schema:
                    data_path = os.path.join(data_path, node.name)
            file_path = data_path + parent_ro.import_file_ext
            if not os.path.exists(file_path):                
                file_msg = f"File does not exist for {node.name} ({node.id}): {file_path}"
                print(file_msg)
                return None             
            return file_path
        
        if not isinstance(vr, Variable):
            return vr # Hard-coded value, no VR.
        
        if not process:
            raise ValueError("No process provided to get the value of the VR. AFAIK this only happens when the dynamically generated VR is not yet assigned to a prior Process.")
        
        self_idx = node_lineage.index(self)
        base_node = node_lineage[self_idx]
        cursor = action.conn.cursor()
        if not isinstance(process, list):
            process = [process]
        found_value = False
        subclasses = DataObject.__subclasses__()
        for pr in lookup_pr:
            for node in node_lineage[self_idx:]:
                sqlquery_raw = f"SELECT str_value FROM data_values WHERE path_id = ? AND vr_id = ? AND pr_id = ?"
                sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id", "pr_id"], single = True, user = True, computer = False)
                            
                params = (node.id, lookup_vr.id, pr.id)            
                result = cursor.execute(sqlquery, params).fetchall()
                lookup_node_name = result[0][0] if len(result) > 0 else None

                if lookup_node_name:
                    sqlquery = """SELECT dataobject_id FROM paths WHERE path LIKE '%"{}"]'""".format(lookup_node_name)
                    result = cursor.execute(sqlquery).fetchall()
                    dobj_id = result[0][0] if len(result) > 0 else None
                    cls = [cls for cls in subclasses if cls.prefix == dobj_id[0:2]][0]
                    try:
                        lookup_node = cls(id = dobj_id, action=action)
                    except:
                        raise ValueError(f"Could not find the node {lookup_node} in the database.")
                    node_lineage = lookup_node.get_node_lineage(action = action)
                    self_idx = node_lineage.index(lookup_node)
                    break
            if lookup_node:
                node = lookup_node                
                break
        for pr in process:
            for node in node_lineage[self_idx:]:
                sqlquery_raw = f"SELECT action_id_num FROM data_values WHERE path_id = ? AND vr_id = ? AND pr_id = ?"
                sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id", "pr_id"], single = True, user = True, computer = False)
                            
                params = (node.id, vr.id, pr.id)            
                result = cursor.execute(sqlquery, params).fetchall()
                if len(result) > 0:
                    break
            if len(result) == 0:
                # print(f"The VR {vr.name} ({vr.id}) has never been associated with the data object {base_node.name} ({base_node.id}) for PR {pr.id}.")
                continue
            is_active = result[0][0]
            # if is_active == 0:
            #     print(f"The VR {vr.name} is not currently associated with the data object {base_node.name} ({base_node.id}) for PR {pr.id}.")
            #     continue
            
            # 2. Load the data hash from the database.            
            sqlquery_raw = "SELECT data_blob_hash, pr_id, numeric_value, str_value FROM data_values WHERE path_id = ? AND vr_id = ? AND pr_id = ?"
            params = (node.id, vr.id, pr.id)
            sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id", "pr_id"], single = True, user = True, computer = False)        
            result = cursor.execute(sqlquery, params).fetchall()
            if len(result) == 0:
                print(f"The VR {vr.name} does not have a value for the data object {base_node.name} ({base_node.id}) from Process {pr.id}.")
                continue
            if len(result) > 1:
                print(f"The VR {vr.name} has multiple values for the data object {base_node.name} ({base_node.id}) from Process {pr.id}.")
                continue
            pr_ids = [x[1] for x in result]
            pr_idx = None
            for pr_id in pr_ids:
                pr_idx = None
                try:
                    pr_idx = pr_ids.index(pr_id)
                except:
                    pass
                if pr_idx is not None:
                    break
            if pr_idx is None:
                print(f"The VR {vr.name} does not have a value set for the data object {base_node.name} ({base_node.id}) from any Process provided.")
                continue
            data_hash = result[pr_idx][0]

            # 3. Get the value from the data_values table. 
            if data_hash is not None:
                pool_data = SQLiteConnectionPool(name = "data")
                conn_data = pool_data.get_connection()
                cursor_data = conn_data.cursor()
                sqlquery = "SELECT data_blob FROM data_values_blob WHERE data_blob_hash = ?"        
                params = (data_hash,)
                pickled_value = cursor_data.execute(sqlquery, params).fetchone()[0]
                value = pickle.loads(pickled_value)
                pool_data.return_connection(conn_data)
            else:
                numeric_value = result[pr_idx][2]
                str_value = result[pr_idx][3]
                if numeric_value is not None:
                    value = numeric_value
                else: # Omitting criteria here allows for str_value and numeric_value to both be None.
                    value = str_value

            found_value = True
            if value is not None:
                break # Allow a None value to go through, but gives every opportunity to look for another value first.
        if not found_value:
            raise ValueError(f"The VR {vr.name} does not have a value set for the data object {base_node.name} ({base_node.id}) from any Process provided.")        
        return value
    
    def _set_vr_values(self, vr_values: dict, pr: "source_type", action: Action = None) -> None:
        """Top level function to set the values of the VR attributes.
        Called like: self._set_vr_values(research_object, vr_values, pr_id, action)
        vr_values: dict with keys of type Variable and values of the values to set.
        pr_id: str, the process id."""
        from ResearchOS.code_runner import CodeRunner
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        if not vr_values:
            return
        commit = False
        if action is None:
            commit = True
            action = Action(name = "set_vr_values")
        # 1. Get hash or value of each value.
        vr_hashes_dict = {}
        for vr, raw_value in vr_values.items():
            value = convert_var(raw_value, CodeRunner.matlab_numeric_types)
            data_blob = None
            data_blob_hash = None
            scalar_value = None
            # Check if the value is a scalar.            
            try:
                if isinstance(value, (type(None), str, int, float, bool)):
                    tmp = json.dumps(value) # Scalars
                elif isinstance(value, np.ndarray):
                    assert len(value) == 1 # Scalar numpy arrays
                else:
                    assert False # Blob value.
                scalar_value = value                
            except:                
                data_blob = pickle.dumps(value, protocol = 4)
                data_blob_hash = sha256(data_blob).hexdigest()
            vr_hashes_dict[vr] = {"hash": data_blob_hash, "blob": data_blob, "scalar_value": scalar_value}

        # 2. Check which VR's hashes are already in the data database so as not to duplicate a value/hash (primary key)
        pool_data = SQLiteConnectionPool(name = "data")
        conn_data = pool_data.get_connection()
        cursor_data = conn_data.cursor()
        vr_hashes = tuple(set([vr["hash"] for vr in vr_hashes_dict.values()])) # Get unique values.
        sqlquery = "SELECT data_blob_hash FROM data_values_blob WHERE data_blob_hash IN ({})".format(", ".join("?" * len(vr_hashes)))
        result = cursor_data.execute(sqlquery, vr_hashes).fetchall()
        pool_data.return_connection(conn_data)
        vr_hashes_prev_exist = []        
        for vr in vr_hashes_dict:
            for row in result:
                hash = row[0]
                if hash is not None and hash == vr_hashes_dict[vr]["hash"]:
                    vr_hashes_prev_exist.append(vr)
                    break    

        # 2. Insert the values into the proper tables.
        pr_id = pr
        if isinstance(pr, (Process, Logsheet)):
            pr_id = pr.id
        for vr in vr_hashes_dict:
            blob_params = (vr_hashes_dict[vr]["hash"], vr_hashes_dict[vr]["blob"])
            blob_pk = blob_params
            vr_id = vr
            if isinstance(vr, Variable):
                vr_id = vr.id
            if isinstance(vr_hashes_dict[vr]["scalar_value"], str):
                vr_value_params = (action.id_num, self.id, pr_id, vr_id, vr_hashes_dict[vr]["hash"], vr_hashes_dict[vr]["scalar_value"], None)
            else:
                vr_value_params = (action.id_num, self.id, pr_id, vr_id, vr_hashes_dict[vr]["hash"], None, vr_hashes_dict[vr]["scalar_value"])
            vr_value_pk = vr_value_params
            # Don't insert the data_blob if it already exists.
            if not vr in vr_hashes_prev_exist and vr_hashes_dict[vr]["hash"] is not None:
                if not action.is_redundant_params(self.id, "data_value_in_blob_insert", blob_pk, group_name = "robj_vr_attr_insert"):
                    action.add_sql_query(self.id, "data_value_in_blob_insert", blob_params, group_name = "robj_vr_attr_insert")
            if not action.is_redundant_params(self.id, "vr_value_for_dobj_insert", vr_value_pk, group_name = "robj_vr_attr_insert"):
                action.add_sql_query(self.id, "vr_value_for_dobj_insert", vr_value_params, group_name = "robj_vr_attr_insert")

        if commit:
            action.commit = commit
            action.exec = True
            action.execute()

    def get_node_lineage(self, dobj_ids: list = None, paths: list = None, action: Action = None) -> list:
        """Get the lineage of the DataObject node.
        """
        if type(self).__name__ == "Dataset":
            return [self]
        node_id = self.id
        return_conn = False
        if action is None:
            return_conn = True
            pool = SQLiteConnectionPool()
            conn = pool.get_connection()
        else:
            conn = action.conn
        sqlquery = "SELECT dataobject_id, path FROM paths"
        cursor = conn.cursor()
        result = cursor.execute(sqlquery).fetchall()
        paths_from_db = [json.loads(x[1]) for x in result]
        dobj_ids_from_db = [x[0] for x in result]        
        if not dobj_ids:
            dobj_ids = dobj_ids_from_db
            paths = paths_from_db
        node_id_idx = dobj_ids.index(node_id)
        path = paths[node_id_idx]
        path = path[0:path.index(self.name)+1]

        node_lineage = []
        for idx in range(len(path)):
            row_idx = paths.index(path[0:idx+1])
            node_lineage.append(dobj_ids[row_idx])
        subclasses = DataObject.__subclasses__()
        node_lineage_objs = []
        for node_id in node_lineage:
            cls = [cls for cls in subclasses if cls.prefix == node_id[0:2]][0]
            anc_node = cls(id = node_id, action=action)
            node_lineage_objs.append(anc_node)
        if return_conn:
            pool.return_connection(conn)
        return node_lineage_objs[::-1] # Because expecting smallest first.
    
    def get_node_info(self, action: Action) -> dict:
        """Provide the node lineage information to the scientific code. Helpful for conditional debugging in the scientific code."""
        # classes = DataObject.__subclasses__()
        # cls = [cls for cls in classes if cls.prefix == self.id[0:2]][0]
        node_lineage = self.get_node_lineage(action=action)
        info = {}        
        for node in node_lineage:
            prefix = node.prefix
            info[prefix] = {}
            info[prefix]["name"] = node.name
            info[prefix]["id"] = node.id
        return info

def load_data_object_classes() -> None:
    """Import all data object classes from the config.data_objects_path.
    """
    tomlhandler = TOMLHandler("pyproject.toml")
    data_objects_import_path = tomlhandler.get("tool.researchos.paths.research_objects.dataobject")
    data_objects_import_abs_path = tomlhandler.make_abs_path(data_objects_import_path)

    if data_objects_import_path.endswith(".py"):
        data_objects_import_path = data_objects_import_path[:-3].replace("/", ".")
    else:
        data_objects_import_path = data_objects_import_path.replace("/", ".")

    if os.path.exists(data_objects_import_abs_path):
        import importlib
        data_objects_module = importlib.import_module(data_objects_import_path)
        for name in dir(data_objects_module):
            if not name.startswith("__"):                
                globals()[name] = getattr(data_objects_module, name)

def get_all_dataobjects_vrs(action: Action) -> list:
    """Gets all data objects & their corresponding VR's from the database at once."""
    # Get all VR's and their path ID's.
    sqlquery_raw = "SELECT path_id, vr_id FROM vr_dataobjects WHERE is_active = 1"
    sub_sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id"], single = True, user = True, computer = False)
    sqlquery = "SELECT paths.dataobject_id, subquery.vr_id FROM ({}) AS subquery JOIN paths ON subquery.path_id = paths.path_id".format(sub_sqlquery)
    cursor = action.conn.cursor()
    result = cursor.execute(sqlquery).fetchall()

    dobjs_vr_list = {}
    unique_dobj_ids = list(set([x[0] for x in result]))
    for dobj_id in unique_dobj_ids:
        dobjs_vr_list[dobj_id] = []
        [dobjs_vr_list[dobj_id].append(Variable(id = x[1])) for x in result if x[0] == dobj_id]
    return dobjs_vr_list