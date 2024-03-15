from typing import TYPE_CHECKING, Any
import logging, time
import os
from datetime import datetime, timezone
import json

import networkx as nx
import numpy as np

if TYPE_CHECKING:
    from .action import Action
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.DataObjects.dataset import Dataset    

from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.var_converter import convert_var
from ResearchOS.utils.suppress_output import suppress_stdout_stderr

class ProcessRunner():

    eng = None
    matlab = None
    matlab_numeric_types = []
    dataset_object_graph: nx.MultiDiGraph = None

    def __init__(self, process: "Process", action: "Action", schema_id: str, schema_graph: nx.MultiDiGraph, dataset: "Dataset", subset_graph, matlab_loaded: bool, eng, force_redo: bool):
        self.process = process
        self.action = action
        self.schema_id = schema_id
        self.schema_graph = schema_graph
        self.schema_order = list(nx.topological_sort(schema_graph))
        self.dataset = dataset
        self.subset_graph = subset_graph
        self.matlab_loaded = matlab_loaded
        self.eng = eng
        self.force_redo = force_redo

    def get_node_info(self, node_id: str) -> dict:
        """Provide the node lineage information for conditional debugging in the scientific code."""
        anc_nodes = nx.ancestors(self.subset_graph, node_id)
        anc_nodes_list = [node for node in nx.topological_sort(self.subset_graph.subgraph(anc_nodes))][::-1]
        node_lineage = [node_id] + [node for node in anc_nodes_list]
        info = {}        
        info["lineage"] = {}
        classes = DataObject.__subclasses__()
        assert len(node_lineage) == len(self.schema_order)
        for node_id in node_lineage:
            cls = [cls for cls in classes if cls.prefix == node_id[0:2]][0]
            node = cls(id = node_id, action = self.action)
            prefix = cls.prefix
            info["lineage"][prefix] = {}
            info["lineage"][prefix]["name"] = node.name
            info["lineage"][prefix]["id"] = node.id

        return info
    
    def run_batch(self, batch_id: str, batch_value: dict = {}, G: nx.MultiDiGraph = nx.MultiDiGraph()) -> None:
        """Get all of the input values for the batch of nodes and run the process on the whole batch.
        """
        if batch_value is None and self.process.batch is None:
            self.run_node(batch_id, G)
            return
        # If there's only one key, then it's the batch ID.
        if len(batch_dict) == 1 and all(isinstance(value, dict) for value in batch_dict.values()):
            batch_id = list(batch_dict.keys())[0]
            batch_dict = batch_dict[batch_id]

        data_subclasses = DataObject.__subclasses__()
        for key in batch_dict.keys():
            cls = [cls for cls in data_subclasses if cls.prefix == key[0:2]][0]
            self.node = cls(id = key)
            run_process, vr_values_in = self.check_if_run_node(key, G) # Verify whether this node should be run or skipped, and get input variables. 
            if run_process is False:
                break
            batch_dict[key] = vr_values_in

        input_dict = {k: {} for k in self.process.input_vrs.keys()}
        for k in input_dict.keys():
            for node_id in batch_dict.keys():
                input_dict[k][node_id] = batch_dict[node_id][k]

        # Run the process on the batch of nodes.
        self.compute_and_assign_outputs(input_dict, self.process, {})
        

    def run_node(self, node_id: str, G: nx.MultiDiGraph) -> None:
        """Run the process on the given node ID.
        """
        start_run_time = time.time()
        self.node_id = node_id
        pr = self.process
        node = pr.level(id = node_id, action = self.action)
        self.node = node
        
        run_process, vr_values_in = self.check_if_run_node(node_id, G) # Verify whether this node should be run or skipped.
        skip_msg = None
        if not vr_values_in:            
            skip_msg = f"File does not exist for {node.name} ({node.id}), skipping."
        if vr_values_in is not None and not (self.force_redo or run_process):
            skip_msg = f"Already done {node.name} ({node.id}), skipping."
        if skip_msg is not None:
            print(skip_msg)
            return
        
        node_info = self.get_node_info(node_id)
        run_msg = f"Running {node.name} ({node.id})."
        print(run_msg)
        is_batch = pr.batch is not None
        assert is_batch == False
        self.compute_and_assign_outputs(vr_values_in, pr, node_info, is_batch)

        self.action.commit = True
        self.action.exec = True
        self.action.execute(return_conn = False)        

        done_run_time = time.time()
        done_msg = f"Running {node.name} ({node.id}) took {round(done_run_time - start_run_time, 3)} seconds."
        logging.info(done_msg)
        print(done_msg)

    def compute_and_assign_outputs(self, vr_values_in: dict, pr: "Process", info: dict, is_batch: bool = False, ) -> None:
        """Assign the output variables to the DataObject node.
        """
        # NOTE: For now, assuming that there is only one return statement in the entire method.  
        if pr.is_matlab:
            if not self.matlab_loaded:
                raise ValueError("MATLAB is not loaded.")            
            fcn = getattr(self.eng, pr.mfunc_name)                        
        else:
            fcn = getattr(pr, pr.method)

        if not is_batch:
            vr_vals_in = list(vr_values_in.values())
            if self.num_inputs > len(vr_vals_in): # There's an extra input open.
                vr_vals_in.append(info)
        else:
            # Convert the vr_values_in to the right format.
            pass

        try:
            vr_values_out = fcn(*vr_vals_in, nargout=len(pr.output_vrs))
        except ProcessRunner.matlab.engine.MatlabExecutionError as e:
            if "ResearchOS:" not in e.args[0]:
                print("'ResearchOS:' not found in error message, ending run.")
                raise e
            return # Do not assign anything, because nothing was computed!
                
        if not isinstance(vr_values_out, tuple):
            vr_values_out = (vr_values_out,)
        if len(vr_values_out) != len(pr.output_vrs):
            raise ValueError("The number of variables returned by the method must match the number of output variables registered with this Process instance.")
            
        # Set the output variables for this DataObject node.
        idx = -1 # For MATLAB. Requires that the args are in the proper order.
        kwargs_dict = {}
        output_var_names_in_code = [vr for vr in pr.output_vrs.keys()]
        for vr_name, vr in pr.output_vrs.items():
            if not pr.is_matlab:
                idx = output_var_names_in_code.index(vr_name) # Ensure I'm pulling the right VR name because the order of the VR's coming out and the order in the output_vrs dict are probably different.
            else:
                idx += 1
            # Search through the variable to look for any matlab numeric types and convert them to numpy arrays.
            kwargs_dict[vr] = convert_var(vr_values_out[idx], ProcessRunner.matlab_numeric_types) # Convert any matlab.double to numpy arrays. (This is a recursive function.)

        self.node._setattrs({}, kwargs_dict, action = self.action, pr_id = self.process.id)
        
    def check_if_run_node(self, node_id: str, G: nx.MultiDiGraph) -> bool:
        """Check whether to run the Process on the given node ID. If False, skip. If True, run.
        """
        self.node_id = node_id                      

        input_vrs, input_vrs_names_dict = self.get_input_vrs(G)
        if input_vrs is None:
            return (False, input_vrs) # The file does not exist. Skip this node.        
        earliest_output_vr_time = self.get_earliest_output_vr_time()
        latest_input_vr_time = self.get_latest_input_vr_time(input_vrs, input_vrs_names_dict)
        if latest_input_vr_time < earliest_output_vr_time:
            return (False, input_vrs) # Do NOT run the process.
        else:
            return (True, input_vrs) # Run the process
        
    def get_node_lineage(self, node: DataObject, G: nx.MultiDiGraph) -> list:
        """Get the lineage of the DataObject node.
        """
        anc_nodes = nx.ancestors(G, node)
        if len(anc_nodes) == 0:
            return [node]
        anc_nodes_list = [node for node in nx.topological_sort(G.subgraph(anc_nodes))][::-1]
        anc_nodes = []
        for idx, level in enumerate(self.schema_order[1:-1]):
            anc_nodes.append(anc_nodes_list[idx])                         
        node_lineage = [node] + anc_nodes + [self.dataset] # Smallest to largest.
        return node_lineage

    def get_input_vrs(self, G: nx.MultiDiGraph) -> dict:
        """Load the input variables.
        """
        pr = self.process
        node = self.node
        start_input_vrs_time = time.time()
        logging.info(f"Loading input VR's for {node.name} ({node.id}).")
        # Get the values for the input variables for this DataObject node. 
        # Get the lineage so I can get the file path for import functions and create the lineage.        
        node_lineage = self.get_node_lineage(node, G)     
        vr_values_in = {}     
        input_vrs_names_dict = {}
        lookup_vrs = self.process.lookup_vrs
        for var_name_in_code, vr in pr.input_vrs.items():
            vr_found = False
            input_vrs_names_dict[var_name_in_code] = vr
            node_lineage_use = node_lineage
            curr_node = node_lineage_use[0] # Always the lowest level to start.

            if var_name_in_code == pr.import_file_vr_name:
                continue # Skip the import file variable.

            # Hard-coded input variable.
            if type(vr) is not dict and vr.hard_coded_value is not None: 
                vr_values_in[var_name_in_code] = vr.hard_coded_value
                vr_found = True
                continue   

            # Check if this variable should be pulled from another DataObject.
            for lookup_var_name_in_code, lookup_vr_dict in lookup_vrs.items():
                for lookup_vr, lookup_var_names_in_code in lookup_vr_dict.items():
                    if var_name_in_code not in lookup_var_names_in_code:
                        continue
                    if lookup_var_name_in_code not in self.process.vrs_source_pr:
                        raise ValueError(f"Lookup variable {lookup_var_name_in_code} not found in Process's vrs_source_pr.")
                    lookup_process = self.process.vrs_source_pr[lookup_var_name_in_code]                    
                    lookup_dataobject_name, vr_found = curr_node.load_vr_value(lookup_vr, self.action, lookup_process, lookup_var_name_in_code)
                    if lookup_dataobject_name is None:   
                        return (None, None) # The file does not exist. Skip this node.
                    
                    # Currently assumes that all DataObjects have unique names across the entire dataset.
                    lookup_dataobject = [n for n in G.nodes if n.name == lookup_dataobject_name][0]
                    node_lineage_use = self.get_node_lineage(lookup_dataobject, G)
                    curr_node = node_lineage_use[0] # Replace the current node if needed.
                    break
                    

            # Get the DataObject attribute if needed.
            if isinstance(vr, dict):
                dobj_level = [key for key in vr.keys()][0]
                dobj_attr_name = [value for value in vr.values()][0]
                data_object = [tmp_node for tmp_node in node_lineage_use if isinstance(tmp_node, dobj_level)][0]
                vr_values_in[var_name_in_code] = getattr(data_object, dobj_attr_name)
                continue                                 

            # Not hard-coded input variable.
            
            value, vr_found = curr_node.load_vr_value(vr, self.action, self.process, var_name_in_code)
            if value is None and not vr_found:
                return (None, None)
            vr_values_in[var_name_in_code] = value
            vr_found = True
            if not vr_found:
                raise ValueError(f"Variable {vr.name} ({vr.id}) not found in __dict__ of {node}.")
                    
        done_input_vrs_time = time.time()
        logging.debug(f"Loading input VR's for {node.name} ({node.id}) took {done_input_vrs_time - start_input_vrs_time} seconds.")

        # Handle if this node has an import file variable.
        data_path = self.dataset.dataset_path        
        for node in node_lineage[1::-1]:
            data_path = os.path.join(data_path, node.name)        
        # TODO: Make the name of this variable not hard-coded.
        if pr.import_file_vr_name is not None:
            file_path = data_path + pr.import_file_ext
            if not os.path.exists(file_path):                
                return (None, None)
            vr_values_in[pr.import_file_vr_name] = file_path        

        return (vr_values_in, input_vrs_names_dict)
    
    def check_output_vrs_active(self) -> bool:
        """Check if the output variables are active.

        Returns:
            bool: True if the output variables are active, False otherwise.
        """
        pr = self.process
        node = self.node        
        # Get the latest action_id where this DataObject was assigned a value to any of the output VR's AND the connection between each output VR and this DataObject is active.
        # If the connection between each output VR and this DataObject is not active, then run the process.
        cursor = self.action.conn.cursor()
        output_vr_ids = [vr.id for vr in pr.output_vrs.values()]
        run_process = False

        # Check that all output VR connections are active.
        sqlquery_raw = "SELECT is_active FROM vr_dataobjects WHERE dataobject_id = ? AND vr_id IN ({})".format(",".join("?" * len(output_vr_ids)))
        sqlquery = sql_order_result(self.action, sqlquery_raw, ["is_active"], single = True, user = True, computer = False)
        params = ([node.id] + output_vr_ids)
        result = cursor.execute(sqlquery, params).fetchall()            
        all_vrs_active = all([row[0] for row in result if row[0] == 1]) # Returns True if result is empty.
        run_process = None
        if not result or not all_vrs_active:
            run_process = True

        return run_process
        
    def get_earliest_output_vr_time(self) -> datetime:
        # Get the latest action_id
        cursor = self.action.conn.cursor()
        pr = self.process
        output_vr_ids = [vr.id for vr in pr.output_vrs.values()]
        sqlquery_raw = "SELECT action_id FROM data_values WHERE dataobject_id = ? AND vr_id IN ({})".format(",".join("?" * len(output_vr_ids)))
        sqlquery = sql_order_result(self.action, sqlquery_raw, ["dataobject_id", "vr_id"], single = True, user = True, computer = False) # The data is ordered. The most recent action_id is the first one.
        params = tuple([self.node.id] + output_vr_ids)
        result = cursor.execute(sqlquery, params).fetchall() # The latest action ID
        if len(result) == 0:
            output_vrs_earliest_time = datetime.min.replace(tzinfo=timezone.utc)
        else:                
            most_recent_action_id = result[0][0] # Get the latest action_id.
            sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
            params = (most_recent_action_id,)
            result = cursor.execute(sqlquery, params).fetchall()
            output_vrs_earliest_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")

        return output_vrs_earliest_time
    
    def get_latest_input_vr_time(self, vr_vals_in: dict, input_vrs_names_dict: dict) -> datetime:
        """Get the datetime when the last input VR was modified.

        Returns:
            datetime: _description_
        """
        cursor = self.action.conn.cursor()
        # pr = self.process
        # Check if the values for all the input variables are up to date. If so, skip this node.
        # check_vr_values_in = {vr_name: vr_val for vr_name, vr_val in vr_vals_in.items()}            
        input_vrs_latest_time = datetime.min.replace(tzinfo=timezone.utc)
        for vr_name in vr_vals_in:
            if vr_name == self.process.import_file_vr_name:
                continue # Special case. Skip the import file variable.

            vr = input_vrs_names_dict[vr_name]
            # Hard coded. Return when the hard coded value was last set.
            if vr.hard_coded_value is not None:
                sqlquery_raw = "SELECT action_id FROM simple_attributes WHERE object_id = ? AND attr_id = ? AND attr_value = ?"
                sqlquery = sql_order_result(self.action, sqlquery_raw, ["object_id", "attr_id", "attr_value"], single = True, user = True, computer = False)
                attr_id = ResearchObjectHandler._get_attr_id("hard_coded_value")
                params = (vr.id, attr_id, json.dumps(vr.hard_coded_value)) # Has to be JSON encoded.
                result = cursor.execute(sqlquery, params).fetchall()
                if len(result) == 0:
                    return datetime.max.replace(tzinfo=timezone.utc) # Force the process to run.
                sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
                params = (result[0][0],)
                result = cursor.execute(sqlquery, params).fetchall()
                new_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")
                if new_time > input_vrs_latest_time:
                    input_vrs_latest_time = new_time
                continue
            
            # Dynamic input variable. Return when the data blob hash was last set for this VR & data object.
            sqlquery_raw = "SELECT action_id, data_blob_hash FROM data_values WHERE vr_id = ? AND schema_id = ?"
            sqlquery = sql_order_result(self.action, sqlquery_raw, ["vr_id", "schema_id"], single = True, user = True, computer = False)
            params = (vr.id, self.schema_id)
            result = cursor.execute(sqlquery, params).fetchall()
            if len(result) == 0:
                return datetime.max.replace(tzinfo=timezone.utc) # Force the process to run.

            latest_action_id = result[0][0]
            sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
            params = (latest_action_id,)
            result = cursor.execute(sqlquery, params).fetchall()
            new_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")
            if new_time > input_vrs_latest_time:
                input_vrs_latest_time = new_time
        return input_vrs_latest_time