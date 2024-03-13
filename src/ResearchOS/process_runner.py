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

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.var_converter import convert_var, convert_py_to_matlab

class ProcessRunner():

    eng = None

    def __init__(self, process: "Process", action: "Action", schema_id: str, schema_order: list, dataset: "Dataset", subset_graph, matlab_loaded: bool, eng, force_redo: bool):
        self.process = process
        self.action = action
        self.schema_id = schema_id
        self.schema_order = schema_order
        self.dataset = dataset
        self.subset_graph = subset_graph
        self.matlab_loaded = matlab_loaded
        self.eng = eng
        self.force_redo = force_redo

    def run_node(self, node_id: str) -> None:
        """Run the process on the given node ID.
        """
        start_run_time = time.time()
        self.node_id = node_id
        pr = self.process
        node = pr.level(id = node_id, action = self.action)
        self.node = node        
        
        run_process, vr_values_in = self.check_if_run_node(node_id) # Verify whether this node should be run or skipped.
        skip_msg = None
        if not vr_values_in:            
            skip_msg = f"File does not exist for {node.name} ({node.id}), skipping."
        if vr_values_in is not None and not (self.force_redo or run_process):
            skip_msg = f"Already done {node.name} ({node.id}), skipping."
        if skip_msg is not None:
            print(skip_msg)
            return
        
        run_msg = f"Running {node.name} ({node.id})."
        print(run_msg)
        self.compute_and_assign_outputs(vr_values_in, pr)

        self.action.commit = True
        self.action.exec = True
        self.action.execute(return_conn = False)        

        done_run_time = time.time()
        done_msg = f"Running {node.name} ({node.id}) took {round(done_run_time - start_run_time, 3)} seconds."
        logging.info(done_msg)
        print(done_msg)

    def compute_and_assign_outputs(self, vr_values_in: dict, pr: "Process") -> None:
        """Assign the output variables to the DataObject node.
        """
        # NOTE: For now, assuming that there is only one return statement in the entire method.        
        if pr.is_matlab:
            if not self.matlab_loaded:
                raise ValueError("MATLAB is not loaded.")
            vr_vals_in = list(vr_values_in.values())
            # for idx in range(len(vr_vals_in)):
            #     vr_vals_in = convert_py_to_matlab(vr_vals_in[idx], self.matlab_numeric_types)
            fcn = getattr(self.eng, pr.mfunc_name)
            vr_values_out = fcn(*vr_vals_in, nargout=len(pr.output_vrs))                               
        else:
            vr_values_out = pr.method(**vr_values_in) # Ensure that the method returns a tuple.
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
            kwargs_dict[vr] = convert_var(vr_values_out[idx], self.matlab_numeric_types) # Convert any matlab.double to numpy arrays. (This is a recursive function.)

        vr_values_out = []
        self.node._setattrs({}, kwargs_dict, action = self.action, pr_id = self.process.id)
        kwargs_dict = {}    
        
    def check_if_run_node(self, node_id: str) -> bool:
        """Check whether to run the Process on the given node ID. If False, skip. If True, run.
        """
        self.node_id = node_id                      

        input_vrs, input_vrs_names_dict = self.get_input_vrs()
        if input_vrs is None:
            return (False, input_vrs) # The file does not exist. Skip this node.        
        earliest_output_vr_time = self.get_earliest_output_vr_time()
        latest_input_vr_time = self.get_latest_input_vr_time(input_vrs, input_vrs_names_dict)
        if latest_input_vr_time < earliest_output_vr_time:
            return (False, input_vrs) # Run the process.
        else:
            return (True, input_vrs)

    def get_input_vrs(self) -> dict:
        """Load the input variables.
        """
        pr = self.process
        node = self.node
        start_input_vrs_time = time.time()
        logging.info(f"Loading input VR's for {node.name} ({node.id}).")
        # Get the values for the input variables for this DataObject node. 
        # Get the lineage so I can get the file path for import functions and create the lineage.        
        anc_node_ids = nx.ancestors(self.subset_graph, self.node_id)
        anc_node_ids_list = [node for node in nx.topological_sort(self.subset_graph.subgraph(anc_node_ids))][::-1]
        anc_node_ids_list.remove(self.dataset.id)
        anc_nodes = []
        # Skip the lowest level (the Process level) and the highest level (dataset)
        for idx, level in enumerate(self.schema_order[1:-1]):
            anc_nodes.append(level(id = anc_node_ids_list[idx], action = self.action))          
        vr_values_in = {}        
        node_lineage = [node] + anc_nodes + [self.dataset] # Smallest to largest.        
        input_vrs_names_dict = {}
        for var_name_in_code, vr in pr.input_vrs.items():
            vr_found = False
            input_vrs_names_dict[var_name_in_code] = vr
            if var_name_in_code == pr.import_file_vr_name:
                continue # Skip the import file variable.

            # Get the DataObject attribute if needed.
            if isinstance(vr, dict):
                dobj_level = [key for key in vr.keys()][0]
                dobj_attr_name = [value for value in vr.values()][0]
                data_object = [tmp_node for tmp_node in node_lineage if isinstance(tmp_node, dobj_level)][0]
                vr_values_in[var_name_in_code] = getattr(data_object, dobj_attr_name)
                continue
            
            # Hard-coded input variable.
            if vr.hard_coded_value is not None: 
                vr_values_in[var_name_in_code] = vr.hard_coded_value
                vr_found = True
                continue            

            # Not hard-coded input variable.
            curr_node = [tmp_node for tmp_node in node_lineage if isinstance(tmp_node, vr.level)][0]
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