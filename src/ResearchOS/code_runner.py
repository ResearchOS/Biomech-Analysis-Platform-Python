from typing import TYPE_CHECKING, Optional
from datetime import datetime, timezone
import copy
import os
import json
import time

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.DataObjects.data_object import DataObject

from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.DataObjects.dataset import Dataset

from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.validator import Validator
from ResearchOS.PipelineObjects.subset import Subset
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class CodeRunner():

    matlab_eng = None
    matlab = None
    matlab_numeric_types = []
    dataset_object_graph: nx.MultiDiGraph = None

    # def __init__(self, process: "Process", action: "Action", schema_id: str, schema_graph: nx.MultiDiGraph, dataset: "Dataset", subset_graph, matlab_loaded: bool, eng, force_redo: bool):
    #     self.process = process
    #     self.action = action
    #     self.schema_id = schema_id
    #     self.schema_graph = schema_graph
    #     self.schema_order = list(nx.topological_sort(schema_graph))
    #     self.dataset = dataset
    #     self.subset_graph = subset_graph
    #     self.matlab_loaded = matlab_loaded
    #     self.eng = eng
    #     self.force_redo = force_redo

    @staticmethod
    def import_matlab(is_matlab: bool) -> None:
        matlab_loaded = True
        matlab_double_type = type(None)
        matlab_numeric_types = []
        if is_matlab:
            matlab_loaded = False
            if CodeRunner.matlab_eng is None:
                try:            
                    print("Importing MATLAB.")
                    import matlab.engine
                    try:
                        print("Attempting to connect to an existing shared MATLAB session.")
                        print("To share a session run <matlab.engine.shareEngine('ResearchOS')> in MATLAB's Command Window and leave MATLAB open.")
                        CodeRunner.matlab_eng = matlab.engine.connect_matlab(name = "ResearchOS")
                        print("Successfully connected to the shared 'ResearchOS' MATLAB session.")
                    except:
                        print("Failed to connect. Starting MATLAB.")
                        CodeRunner.matlab_eng = matlab.engine.start_matlab()
                    matlab_loaded = True
                    matlab_double_type = matlab.double
                    matlab_numeric_types = (matlab.double, matlab.single, matlab.int8, matlab.uint8, matlab.int16, matlab.uint16, matlab.int32, matlab.uint32, matlab.int64, matlab.uint64)
                    CodeRunner.matlab = matlab
                    CodeRunner.matlab_numeric_types = matlab_numeric_types
                except:
                    print("Failed to import MATLAB.")
                    matlab_loaded = False 
        CodeRunner.matlab_loaded = matlab_loaded
        CodeRunner.matlab_double_type = matlab_double_type

    @staticmethod
    def set_vrs_source_pr(robj: "ResearchObject", action: Action, schema_id: str, default_attrs: dict) -> None:
        from ResearchOS.PipelineObjects.process import Process
        add_vr_names_source_prs = [key for key in robj.input_vrs.keys() if key not in robj.vrs_source_pr.keys()]
        if len(add_vr_names_source_prs) > 0:
            add_vrs_source_prs = [vr.id for name_in_code, vr in robj.input_vrs.items() if name_in_code in add_vr_names_source_prs]
            sqlquery_raw = "SELECT vr_id, pr_id FROM data_values WHERE vr_id IN ({}) AND schema_id = ?".format(",".join(["?" for _ in add_vrs_source_prs]))
            sqlquery = sql_order_result(action, sqlquery_raw, ["vr_id"], single = True, user = True, computer = False)
            params = tuple(add_vrs_source_prs + [schema_id])
            vr_pr_ids_result = action.conn.cursor().execute(sqlquery, params).fetchall()
            vrs_source_prs = robj.vrs_source_pr
            for vr_name_in_code, vr in robj.input_vrs.items():
                for vr_pr_id in vr_pr_ids_result:
                    if vr.id == vr_pr_id[0]:
                        # Same order as input variables.
                        vrs_source_prs[vr_name_in_code] = Process(id = vr_pr_id[1], action = action)

            robj._setattrs(default_attrs, {"vrs_source_pr": vrs_source_prs}, action = action, pr_id = robj.id) 

    @staticmethod
    def get_lowest_level(robj: "ResearchObject", schema_ordered: list) -> Optional[str]:
        """Get the lowest level for this batch."""
        lowest_level_idx = -1
        for pr in robj.vrs_source_pr.values():
            try:
                level = pr.level                
            except: # For Logsheet, defer to current PR level.
                level = robj.level
            level_idx = schema_ordered.index(level)
            if level_idx <= lowest_level_idx:
                continue
            lowest_level_idx = level_idx
            lowest_level = level
            if lowest_level_idx == len(schema_ordered) - 1:
                break
        return lowest_level
    
    @staticmethod
    def get_level_nodes_sorted(robj: "ResearchObject", action: Action, subset_graph: list) -> list:
        """Get the nodes for this level, sorted by name."""
        level_node_ids = [node for node in subset_graph if node.startswith(robj.level.prefix)]
        name_attr_id = ResearchObjectHandler._get_attr_id("name")
        sqlquery_raw = "SELECT object_id, attr_value FROM simple_attributes WHERE attr_id = ? AND object_id LIKE ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["object_id"], single = True, user = True, computer = False)
        params = (name_attr_id, f"{robj.level.prefix}%")
        level_nodes_ids_names = action.conn.cursor().execute(sqlquery, params).fetchall()
        level_nodes_ids_names.sort(key = lambda x: x[1])
        level_node_ids_sorted = [row[0] for row in level_nodes_ids_names if row[0] in level_node_ids]
        return level_node_ids_sorted
    
    @staticmethod
    def get_batches_dict_to_run(robj: "ResearchObject", subset_graph: nx.MultiDiGraph, schema_ordered: list, lowest_level: str, level_node_ids_sorted: list) -> dict:
        if robj.batch is not None:
            all_batches_graph = CodeRunner.get_batch_graph(robj.batch, subset_graph, schema_ordered, lowest_level)

            # Parse the MultiDiGraph to get the batches to run.
            if robj.batch is None or len(robj.batch) == 0:
                level = Dataset
            else:
                level = robj.batch[0]
            batches_dict_to_run = nx.to_dict_of_dicts(all_batches_graph)
            del_keys = []        
            for key in batches_dict_to_run.keys():
                if not key.startswith(level.prefix):
                    del_keys.append(key)
            for key in del_keys:
                del batches_dict_to_run[key]

            # Dict of dicts, where each top-level dict is a batch to run.
            # Get a top level node.
            top_level_node = [n for n in all_batches_graph.nodes() if all_batches_graph.in_degree(n) == 0][0]
            descendant = list(nx.descendants(all_batches_graph, top_level_node))[0]
            CodeRunner.depth = nx.shortest_path_length(all_batches_graph, top_level_node, descendant)
        else:
            batches_dict_to_run = {node: None for node in level_node_ids_sorted}
            CodeRunner.depth = 0
            all_batches_graph = None
        CodeRunner.lowest_level = lowest_level
        if robj.batch == []:
            highest_level = Dataset
        elif robj.batch is not None:
            highest_level = CodeRunner.batch[0]
        else:
            highest_level = None
        CodeRunner.highest_level = highest_level
        return batches_dict_to_run, all_batches_graph
    
    def get_batch_graph(robj: "ResearchObject", batch: list, subgraph: nx.MultiDiGraph = nx.MultiDiGraph(), schema_ordered: list = [], lowest_level: type = None) -> dict:
        """Get the batch dictionary of DataObject names. Needs to be recursive to account for the nested dicts.
        At the lowest level, if there are multiple DataObjects, they will also be a dict, each being one key, with its value being None.
        Note that the Dataset node should always be in the subgraph."""
        batch_graph = nx.MultiDiGraph()
        # Get lowest Process depth from vrs_source_pr
        lowest_level_idx = schema_ordered.index(lowest_level)

        if batch is None or len(batch) == 0:
            batch = [Dataset]
        if lowest_level not in batch:
            batch = batch + schema_ordered[lowest_level_idx:]

        nodes = [n for b in batch for n in subgraph.nodes() if n.startswith(b.prefix)]
        batch_graph = nx.MultiDiGraph()
        batch_graph.add_nodes_from(nodes)

        # Add the missing edges to the batch graph.
        all_nodes_dict = {cls: [n for n in subgraph.nodes() if n.startswith(cls.prefix)] for cls in batch}
        for idx in range(len(batch) - 1):
            top_level = batch[idx]
            bottom_level = batch[idx + 1]
            top_level_nodes = all_nodes_dict[top_level]
            bottom_level_nodes = all_nodes_dict[bottom_level]
            for n in top_level_nodes:
                for m in bottom_level_nodes:
                    if nx.has_path(subgraph, n, m):
                        batch_graph.add_edge(n, m)
        return batch_graph
    
    def prep_for_run(self, robj: "ResearchObject", action: Action, force_redo: bool = False) -> None:
        """Do all of the prep for running a Process/Plot/Stats object."""        
        default_attrs = DefaultAttrs(robj).default_attrs 
        validator = Validator(robj, action)
        validator.validate(robj.__dict__, default_attrs)

        self.action = action
        self.pl_obj = robj 
        self.force_redo = force_redo

        ds = Dataset(id = robj.get_dataset_id(), action = action)
        schema_id = robj.get_current_schema_id(ds.id)

        self.dataset = ds
        self.schema_id = schema_id

        CodeRunner.import_matlab(robj.is_matlab)
                
        # 4. Run the method.
        # Get the subset of the data.
        subset = Subset(id = robj.subset_id, action = action)
        subset_graph = subset.get_subset(action)

        self.subset_graph = subset_graph
                
        schema = ds.schema
        schema_graph = nx.MultiDiGraph(schema)                
        
        pool = SQLiteConnectionPool()

        CodeRunner.matlab_loaded = True
        if CodeRunner.matlab is None:
            CodeRunner.matlab_loaded = False              

        num_inputs = 0
        if CodeRunner.matlab_loaded and robj.is_matlab:
            CodeRunner.matlab_eng.addpath(robj.mfolder) # Add the path to the MATLAB function. This is necessary for the MATLAB function to be found.
            try:
                num_inputs = CodeRunner.matlab_eng.nargin(robj.mfunc_name, nargout=1)
            except:
                raise ValueError(f"Function {robj.mfunc_name} not found in {robj.mfolder}.")
            
        CodeRunner.num_inputs = num_inputs
            
        if CodeRunner.dataset_object_graph is None:
            CodeRunner.dataset_object_graph = ds.get_addresses_graph(objs = True, action = action)
        G = CodeRunner.dataset_object_graph

        # Set the vrs_source_prs for any var that it wasn't set for.
        CodeRunner.set_vrs_source_pr(robj, action, schema_id, default_attrs)

        # Get the lowest level for this batch.
        schema_ordered = [n for n in nx.topological_sort(schema_graph)]
        self.schema_order = schema_ordered
        lowest_level = CodeRunner.get_lowest_level(robj, schema_ordered)

        level_node_ids_sorted = CodeRunner.get_level_nodes_sorted(robj, action, subset_graph)
            
        batches_dict_to_run, all_batches_graph = CodeRunner.get_batches_dict_to_run(robj, subset_graph, schema_ordered, lowest_level, level_node_ids_sorted)
        return batches_dict_to_run, all_batches_graph, G, pool
    
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
    
    def run_batch(self, batch_id: str, batch_dict: dict = {}, G: nx.MultiDiGraph = nx.MultiDiGraph(), batch_graph: nx.MultiDiGraph = nx.MultiDiGraph) -> None:
        """Get all of the input values for the batch of nodes and run the process on the whole batch.
        """
        if self.pl_obj.batch is None:
            self.run_node(batch_id, G)
            return
        
        lowest_nodes = [node for node in batch_graph.nodes if node.startswith(self.lowest_level.prefix)]

        for node in lowest_nodes:
            self.node_id = node
            self.node = self.lowest_level(id = node)
            run_process, vr_values_in = self.check_if_run_node(node, G)
            if not run_process:
                return False
            for vr_name_in_code, vr_val in vr_values_in.items():
                batch_graph.nodes[node][vr_name_in_code] = vr_val

        # Now that all the input VR's have been gotten, change the node to the one to save the output VR's to.
        self.node = self.highest_level(id = batch_id)
                
        def process_dict(self, batch_graph, input_dict, G, vr_name_in_code: str = None):
            all_keys = list(input_dict.keys())
            is_var_name_in_code = all_keys == list(self.pl_obj.input_vrs.keys())
            do_recurse = is_var_name_in_code or not any([all_keys[i].startswith(self.lowest_level.prefix) for i in range(len(all_keys))])
            if do_recurse and isinstance(input_dict, dict):
                for key, value in input_dict.items():
                    if not is_var_name_in_code:
                        key = None
                    process_dict(self, batch_graph, value, G, key)
                return
                        
            for dataobj_id in all_keys:
                input_dict[dataobj_id] = batch_graph.nodes[dataobj_id][vr_name_in_code]


        input_dict = {node: copy.deepcopy(batch_dict) for node in self.pl_obj.input_vrs.keys()}
        process_dict(self, batch_graph, input_dict, G)

        # Run the process on the batch of nodes.
        is_batch = self.pl_obj.batch is not None
        self.compute_and_assign_outputs(input_dict, self.pl_obj, {}, is_batch)

        self.action.commit = True
        self.action.exec = True
        self.action.execute(return_conn = False)         

    def run_node(self, node_id: str, G: nx.MultiDiGraph) -> None:
        """Run the process on the given node ID.
        """
        start_run_time = time.time()
        self.node_id = node_id
        pl_obj = self.pl_obj
        node = pl_obj.level(id = node_id, action = self.action)
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
        is_batch = pl_obj.batch is not None
        assert is_batch == False
        self.compute_and_assign_outputs(vr_values_in, pl_obj, node_info, is_batch)

        self.action.commit = True
        self.action.exec = True
        self.action.execute(return_conn = False)        

        done_run_time = time.time()
        done_msg = f"Running {node.name} ({node.id}) took {round(done_run_time - start_run_time, 3)} seconds."
        print(done_msg)
        
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
        pr = self.pl_obj
        node = self.node
        # Get the values for the input variables for this DataObject node. 
        # Get the lineage so I can get the file path for import functions and create the lineage.        
        node_lineage = self.get_node_lineage(node, G)     
        vr_values_in = {}     
        input_vrs_names_dict = {}
        lookup_vrs = self.pl_obj.lookup_vrs
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
                    if lookup_var_name_in_code not in self.pl_obj.vrs_source_pr:
                        raise ValueError(f"Lookup variable {lookup_var_name_in_code} not found in Pipeline Object's vrs_source_pr.")
                    lookup_process = self.pl_obj.vrs_source_pr[lookup_var_name_in_code]                    
                    lookup_dataobject_name, vr_found = curr_node.load_vr_value(lookup_vr, self.action, lookup_process, lookup_var_name_in_code, node_lineage_use)
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
            value, vr_found = curr_node.load_vr_value(vr, self.action, self.pl_obj, var_name_in_code, node_lineage_use)
            if value is None and not vr_found:
                return (None, None)
            vr_values_in[var_name_in_code] = value
            vr_found = True
            if not vr_found:
                raise ValueError(f"Variable {vr.name} ({vr.id}) not found in __dict__ of {node}.")

        # Handle if this node has an import file variable.
        data_path = self.dataset.dataset_path        
        for node in node_lineage[1::-1]:
            data_path = os.path.join(data_path, node.name)
        if hasattr(pr, "import_file_vr_name") and pr.import_file_vr_name is not None:
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
        pr = self.pl_obj
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
        pr = self.pl_obj
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
            if vr_name == self.pl_obj.import_file_vr_name:
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