"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from typing import Any, TYPE_CHECKING, Union
import pickle
import os
from hashlib import sha256
import json

import numpy as np
import networkx as nx

if TYPE_CHECKING:    
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.variable import Variable    

from ResearchOS.research_object import ResearchObject
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.Bridges.input import Input

all_default_attrs = {}

computer_specific_attr_names = []

class DataObject(ResearchObject):
    """The parent class for all data objects. Data objects represent some form of data storage, and approximately map to statistical factors."""

    def __delattr__(self, name: str, action: Action = None) -> None:
        """Delete an attribute. If it's a builtin attribute, don't delete it.
        If it's a VR, make sure it's "deleted" from the database."""
        default_attrs = DefaultAttrs(self).default_attrs
        if name in default_attrs:
            raise AttributeError("Cannot delete a builtin attribute.")
        if name not in self.__dict__:
            raise AttributeError("No such attribute.")
        if action is None:
            action = Action(name = "delete_attribute")
        vr_id = self.__dict__[name].id        
        params = (action.id, self.id, vr_id)
        if action is None:
            action = Action(name = "delete_attribute")
        action.add_sql_query(self.id, "vr_to_dobj_insert_inactive", params)
        action.execute()
        del self.__dict__[name]

    def get(self, vr: Union["Variable", "Input"], action: Action, process: "Process" = None, vr_name_in_code: str = None, node_lineage: list = []) -> Any:
        """Load the value of a VR from the database for this data object.

        Args:
            vr (Variable): ResearchOS Variable object to load the value of.
            process (Process): ResearchOS Process object that this data object is part of.
            action (Action): The Action that this is part of.

        Returns:
            Any: The value of the VR for this data object.
        """
        if action is None:
            action = Action(name = "get_vr_value")
        if isinstance(vr, Input):
            process = vr.pr
        func_result = {}
        func_result["input_vrs_names_dict"] = None        
        # 1. Check that the data object & VR are currently associated. If not, throw an error.
        cursor = action.conn.cursor()
        self_idx = node_lineage.index(self)
        for node in node_lineage[self_idx:]:
            sqlquery_raw = "SELECT action_id_num, is_active, vr_id, path_id FROM vr_dataobjects"
            sub_sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id"], single = True, user = True, computer = False)
            sqlquery = "SELECT subquery.action_id_num, subquery.is_active FROM ({}) AS subquery JOIN paths ON subquery.path_id = paths.path_id WHERE paths.dataobject_id = ? AND subquery.vr_id = ?".format(sub_sqlquery)


            # sqlquery_raw = "SELECT path_id, vr_id FROM vr_dataobjects WHERE is_active = 1"
            # sub_sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id"], single = True, user = True, computer = False)
            # sqlquery = "SELECT paths.dataobject_id, subquery.vr_id FROM ({}) AS subquery JOIN paths ON subquery.path_id = paths.path_id".format(sub_sqlquery)
            
            
            params = (node.id, vr.id)            
            result = cursor.execute(sqlquery, params).fetchall()
            if len(result) > 0:
                break
        if len(result) == 0:
            raise ValueError(f"The VR {vr.name} ({vr.id}) is not currently associated with the data object {node.name} ({node.id}).")
        is_active = result[0][1]
        if is_active == 0:
            raise ValueError(f"The VR {vr.name} is not currently associated with the data object {node.id}.")
        
        # 2. Load the data hash from the database.
        if hasattr(process, "vrs_source_pr"):
            pr = process.vrs_source_pr[vr_name_in_code]
        else:
            pr = process
        if not isinstance(pr, list):
            pr = [pr]
        sqlquery_raw = "SELECT data_blob_hash, pr_id, numeric_value, str_value FROM data_values WHERE path_id = ? AND vr_id = ? AND pr_id IN ({})".format(", ".join(["?" for _ in pr]))
        params = (node.id, vr.id) + tuple([pr_elem.id for pr_elem in pr])
        sqlquery = sql_order_result(action, sqlquery_raw, ["path_id", "vr_id"], single = True, user = True, computer = False)        
        result = cursor.execute(sqlquery, params).fetchall()
        if len(result) == 0:
            raise ValueError(f"The VR {vr.name} does not have a value set for the data object {node.id} from Process {process.id}.")
        if len(result) > 1:
            raise ValueError(f"The VR {vr.name} has multiple values set for the data object {node.id} from Process {process.id}.")
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
            raise ValueError(f"The VR {vr.name} does not have a value set for the data object {node.id} from any process provided.")
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
        func_result["do_run"] = True
        func_result["exit_code"] = 0
        func_result["message"] = f"Success in {self.name} ({self.id}). Lookup VR found: {vr_name_in_code} ({vr.id})"
        func_result["vr_values_in"] = value
        return func_result
    
    @staticmethod
    def _set_vr_values(research_object: "ResearchObject", vr_values: dict, action: Action, pr_id: str) -> None:
        """Set the values of the VR attributes."""
        if not vr_values:
            return
        # result = get_all_dataobjects_vrs(action)
        # 1. Get hash of each value.
        vr_hashes_dict = {}
        for vr, value in vr_values.items():
            # Check if the value is a scalar.            
            try:
                if isinstance(value, (type(None), str, int, float, bool)):
                    tmp = json.dumps(value)
                if isinstance(value, np.ndarray):
                    assert len(value) == 1
                scalar_value = value
                data_blob = None
                data_blob_hash = None
            except:
                scalar_value = None
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
        for vr in vr_hashes_dict:
            blob_params = (vr_hashes_dict[vr]["hash"], vr_hashes_dict[vr]["blob"])
            blob_pk = blob_params
            vr_dobj_params = (action.id_num, research_object.id, vr.id)
            vr_dobj_pk = vr_dobj_params
            if isinstance(vr_hashes_dict[vr]["scalar_value"], str):
                vr_value_params = (action.id_num, vr.id, research_object.id, vr_hashes_dict[vr]["hash"], pr_id, vr_hashes_dict[vr]["scalar_value"], None)
            else:
                vr_value_params = (action.id_num, vr.id, research_object.id, vr_hashes_dict[vr]["hash"], pr_id, None, vr_hashes_dict[vr]["scalar_value"])
            vr_value_pk = vr_value_params
            # Don't insert the data_blob if it already exists.
            if not vr in vr_hashes_prev_exist and vr_hashes_dict[vr]["hash"] is not None:
                if not action.is_redundant_params(research_object.id, "data_value_in_blob_insert", blob_pk, group_name = "robj_vr_attr_insert"):
                    action.add_sql_query(research_object.id, "data_value_in_blob_insert", blob_params, group_name = "robj_vr_attr_insert")
            # No danger of duplicating primary keys, so no real need to check if they previously existed. But why not?
            if not action.is_redundant_params(research_object.id, "vr_to_dobj_insert", vr_dobj_pk, group_name = "robj_vr_attr_insert"):
                action.add_sql_query(research_object.id, "vr_to_dobj_insert", vr_dobj_params, group_name = "robj_vr_attr_insert")
            if not action.is_redundant_params(research_object.id, "vr_value_for_dobj_insert", vr_value_pk, group_name = "robj_vr_attr_insert"):
                action.add_sql_query(research_object.id, "vr_value_for_dobj_insert", vr_value_params, group_name = "robj_vr_attr_insert")

    def get_node_lineage(self, node: "DataObject", dobj_ids: list = None, paths: list = None) -> list:
        """Get the lineage of the DataObject node.
        """

        if type(node).__name__ == "Dataset":
            return [node]
        node_id = node.id
        if dobj_ids is None or paths is None:
            sqlquery = "SELECT dataobject_id, path_id FROM paths"
            cursor = node.action.conn.cursor()
            result = cursor.execute(sqlquery).fetchall()
            dobj_ids = [x[0] for x in result if node.name in x[1]]
            paths = [json.loads(x[1]) for x in result if node.name in x[1]]
        node_id_idx = dobj_ids.index(node_id)
        path = paths[node_id_idx]
        path = path[0:path.index(node.name)+1]

        node_lineage = []
        for idx in range(len(path)):
            row_idx = paths.index(path[0:idx+1])
            node_lineage.append(dobj_ids[row_idx])
        subclasses = DataObject.__subclasses__()
        node_lineage_objs = []
        for node_id in node_lineage:
            cls = [cls for cls in subclasses if cls.prefix == node_id[0:2]][0]
            anc_node = cls(id = node_id)
            node_lineage_objs.append(anc_node)
        return node_lineage_objs[::-1] # Because expecting smallest first.

def load_data_object_classes() -> None:
    """Import all data object classes from the config.data_objects_path.
    """
    from ResearchOS.config import Config

    config = Config()
    data_objects_import_path = config.data_objects_path
    main_part, _, extension = data_objects_import_path.rpartition(".")
    main_part = main_part.replace(".", os.sep)

    if extension:
        data_objects_path = main_part + '.' + extension
    else:
        data_objects_path = main_part

    if os.path.exists(data_objects_path):
        import importlib
        data_objects_module = importlib.import_module(config.data_objects_path[:-3])
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