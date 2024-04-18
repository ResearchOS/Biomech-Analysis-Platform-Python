from typing import TYPE_CHECKING, Optional
from datetime import datetime, timezone
import copy
import os
import json
import time

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.DataObjects.dataset import Dataset

from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.validator import Validator
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class CodeRunner():

    matlab_eng = None
    matlab = None
    matlab_numeric_types = []
    dataset_object_graph: nx.MultiDiGraph = None

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
                    print("Failed to import MATLAB. Is it installed in this virtual environment?")
                    matlab_loaded = False 
        CodeRunner.matlab_loaded = matlab_loaded
        CodeRunner.matlab_double_type = matlab_double_type

    @staticmethod
    def get_lowest_level(robj: "ResearchObject", schema_ordered: list) -> Optional[str]:
        """Get the lowest level for this batch."""
        lowest_level_idx = -1
        lowest_level = schema_ordered[-1]
        for input in robj.inputs.values():
            try:
                level = input.pr.level                
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
    def get_level_nodes_sorted(robj: "ResearchObject", action: Action, paths: list, dobj_ids: list, subset_graph: nx.MultiDiGraph) -> list:
        """Get the nodes for this level, sorted by name."""
        # Get the indices in the paths and dobj_ids for the nodes in this subgraph.
        subgraph_idx = []
        for path in paths:
            if all([node in subset_graph.nodes() for node in path]):
                subgraph_idx.append(paths.index(path))
        
        dobj_ids = [dobj_ids[index] for index in subgraph_idx]
        paths = [paths[index] for index in subgraph_idx]
        dataset = [n for n in subset_graph.nodes if subset_graph.in_degree(n) == 0][0]
        paths.append([dataset])
        dobj_ids.append(dataset)
        level_node_ids_with_indices = [(index, dobj) for index, dobj in enumerate(dobj_ids) if dobj.startswith(robj.level.prefix)]        
        level_node_ids = [x[1] for x in level_node_ids_with_indices]
        indices = [x[0] for x in level_node_ids_with_indices]

        # Sort paths and remember the original indices
        level_paths = [paths[idx] for idx in indices] # Isolate just the paths at this level.
        sorted_paths_with_original_indices = sorted(enumerate(level_paths), key = lambda x: x[1][-1])

        # Create a mapping from the original index to the sorted index
        index_map = {original: new for new, (original, _) in enumerate(sorted_paths_with_original_indices)}

        # Sort the level_node_ids
        level_node_ids_sorted = [level_node_ids[index_map[i]] for i in range(len(level_node_ids))]

        return level_node_ids_sorted, paths, dobj_ids
    
    @staticmethod
    def get_batches_dict_to_run(robj: "ResearchObject", subset_graph: nx.MultiDiGraph, schema_ordered: list, lowest_level: str, level_node_ids_sorted: list, paths: list, dobj_ids: list) -> dict:
        if robj.batch is not None:
            all_batches_graph = CodeRunner.get_batch_graph(robj, robj.batch, subset_graph, schema_ordered, lowest_level, paths, dobj_ids)

            # Parse the MultiDiGraph to get the batches to run.
            if robj.batch is None or len(robj.batch) == 0:
                level = Dataset
                batch_list = [Dataset, schema_ordered[-1]]
            elif len(robj.batch) == 1:
                level = robj.batch[0]
                batch_list = [robj.batch[0], schema_ordered[-1]]
            else:
                level = robj.batch[0]
                batch_list = robj.batch

            def graph_to_dict(graph: nx.MultiDiGraph, batches_dict: dict, batch_list: list, node: str, subset_graph: nx.MultiDiGraph, node_lineage: list) -> dict:
                if len(batch_list) == 0:
                    return None
                level = batch_list[0]
                # Get the list of nodes that are connected to this node.
                successors = list(graph.successors(node))
                for n in successors:
                    batches_dict[n] = {}
                    node_lineage.append(n)
                    batches_dict[n] = graph_to_dict(graph, batches_dict[n], batch_list[1:], n, subset_graph, node_lineage)
                return batches_dict
            
            batches_dict_to_run = {}
            top_level_idx = schema_ordered.index(level)
            top_level_nodes = list(set([p[top_level_idx] for p in paths if len(p) > top_level_idx]))
            for node in top_level_nodes:
                batches_dict_to_run[node] = {}
                batches_dict_to_run[node] = graph_to_dict(all_batches_graph, batches_dict_to_run[node], batch_list[1:], node, subset_graph, [node])

            # Dict of dicts, where each top-level dict is a batch to run.
            # Get a top level node.
            any_top_level_node = [n for n in batches_dict_to_run][0]
            leafs = [n for n in all_batches_graph.nodes() if all_batches_graph.out_degree(n) == 0]
            CodeRunner.depth = nx.shortest_path_length(all_batches_graph, any_top_level_node, leafs[0])
        else:
            batches_dict_to_run = {node: None for node in level_node_ids_sorted}
            CodeRunner.depth = 0
            all_batches_graph = None
        CodeRunner.lowest_level = lowest_level
        if robj.batch == []:
            highest_level = Dataset
        elif robj.batch is not None:
            highest_level = robj.batch[0]
        else:
            highest_level = None
        CodeRunner.highest_level = highest_level
        return batches_dict_to_run, all_batches_graph
    
    def get_batch_graph(robj: "ResearchObject", batch: list, subgraph: nx.MultiDiGraph = nx.MultiDiGraph(), schema_ordered: list = [], lowest_level: type = None, paths: list = [], dobj_ids: list = []) -> dict:
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

        batch_idx_in_schema = [schema_ordered.index(cls)+1 for cls in batch]

        paths_in_batch = [path for path in paths if len(path) in batch_idx_in_schema]
        nodes = [path[-1] for path in paths_in_batch]

        batch_graph = nx.MultiDiGraph()
        batch_graph.add_nodes_from(nodes)

        dataset_node = paths[0][0]
        batch_graph.add_node(dataset_node)
        
        for path in paths_in_batch:
            batch_path = [path[idx-1] if idx-1 < len(path) else None for idx in batch_idx_in_schema]
            for idx in range(len(batch_path) - 1):
                edge = (batch_path[idx], batch_path[idx + 1])
                if all([e is not None for e in edge]) and edge not in batch_graph.edges():
                    batch_graph.add_edge(edge[0], edge[1])

        return batch_graph
    
    def prep_for_run(self, robj: "ResearchObject", action: Action, force_redo: bool = False) -> None:
        """Do all of the prep for running a Process/Plot/Stats object."""        
        default_attrs = DefaultAttrs(robj).default_attrs 
        validator = Validator(robj, action)
        validate_dict = {key: value for key, value in robj.__dict__.items() if key not in ["inputs", "outputs"]}
        validator.validate(validate_dict, default_attrs)

        self.action = action
        self.pl_obj = robj 
        self.force_redo = force_redo

        ds = Dataset(id = robj._get_dataset_id(), action = action)

        self.dataset = ds

        CodeRunner.import_matlab(robj.is_matlab)

        # Get the paths and the associated path ID's.
        sqlquery = "SELECT dataobject_id, path FROM paths"
        cursor = action.conn.cursor()
        result = cursor.execute(sqlquery).fetchall()        
        paths = [[ds.id] + json.loads(row[1]) for row in result]
        dobj_ids = [row[0] for row in result]
        # Append Dataset into the paths.
        paths.append([ds.id])
        dobj_ids.append(ds.id)

        self.paths = paths
        self.dobj_ids = dobj_ids
                
        # 4. Run the method.
        # Get the subset of the data.
        subset = robj.subset
        subset_graph = subset.get_subset(action, paths, dobj_ids)

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

        # Get the lowest level for this batch.
        schema_ordered = [n for n in nx.topological_sort(schema_graph)]
        self.schema_order = schema_ordered
        lowest_level = CodeRunner.get_lowest_level(robj, schema_ordered)

        level_node_ids_sorted, paths, dobj_ids = CodeRunner.get_level_nodes_sorted(robj, action, paths, dobj_ids, subset_graph)
            
        batches_dict_to_run, all_batches_graph = CodeRunner.get_batches_dict_to_run(robj, subset_graph, schema_ordered, lowest_level, level_node_ids_sorted, paths, dobj_ids)
        if len(batches_dict_to_run) == 0:
            print("<<< WARNING: NO NODES TO RUN. >>>")
            time.sleep(2)
        return batches_dict_to_run, all_batches_graph, G, pool        
    
    def run_batch(self, batch_id: str, batch_dict: dict = {}, G: nx.MultiDiGraph = nx.MultiDiGraph(), batch_graph: nx.MultiDiGraph = nx.MultiDiGraph) -> None:
        """Get all of the input values for the batch of nodes and run the process on the whole batch.
        """
        if self.pl_obj.batch is None:
            self.run_node(batch_id)
            return
        
        lowest_nodes = [node for node in batch_graph.nodes() if batch_graph.out_degree(node) == 0]

        for node in lowest_nodes:
            self.node_id = node
            path_idx = [idx for idx, path in enumerate(self.paths) if path[-1] == node][0]
            dobj_id = self.dobj_ids[path_idx]
            self.node = self.lowest_level(id = dobj_id)
            result = self.check_if_run_node(node)
            if not result["do_run"]:
                return
            for vr_name_in_code, vr_val in result["vr_values_in"].items():
                batch_graph.nodes[node][vr_name_in_code] = vr_val

        # Now that all the input VR's have been gotten, change the node to the one to save the output VR's to.
        if batch_id == self.dataset.name:
            batch_node = self.dataset.id
        else:
            batch_node_idx = [idx for idx, node in enumerate(self.paths) if node[-1] == batch_id][0]
            batch_node = self.dobj_ids[batch_node_idx]
        self.node = self.highest_level(id = batch_node)
                
        def process_dict(self, batch_graph, input_dict, G, vr_name_in_code: str = None):
            all_keys = list(input_dict.keys())
            if any(isinstance(key, int) for key in all_keys):
                raise ValueError("Check your batch list, no Variables at this level.")
            is_var_name_in_code = all_keys == list(self.pl_obj.input_vrs.keys())
            leaf_nodes = [n for n in batch_graph.nodes() if batch_graph.out_degree(n) == 0]
            do_recurse = is_var_name_in_code or not all([key in leaf_nodes for key in all_keys])
            if do_recurse and isinstance(input_dict, dict):
                for value in input_dict.values():
                    process_dict(self, batch_graph, value, G, vr_name_in_code)
                return
                        
            for dataobj_id in all_keys:
                input_dict[dataobj_id] = batch_graph.nodes[dataobj_id][vr_name_in_code]

        input_dict = {node: copy.deepcopy(batch_dict) for node in self.pl_obj.input_vrs.keys()}
        for var_name_in_code in input_dict:
            curr_vr = self.pl_obj.input_vrs[var_name_in_code]
            if not isinstance(curr_vr, dict) or ("VR" not in curr_vr.keys() and "slice" not in curr_vr.keys()):
                input_dict[var_name_in_code] = result["vr_values_in"][var_name_in_code] # To make it not be a cell array in MATLAB.
            else:
                process_dict(self, batch_graph, input_dict[var_name_in_code], G, var_name_in_code)

        # Run the process on the batch of nodes.
        is_batch = self.pl_obj.batch is not None
        self.compute_and_assign_outputs(input_dict, self.pl_obj, {}, is_batch)

        self.action.commit = True
        self.action.exec = True
        self.action.execute(return_conn = False)         

    def run_node(self, node_id: str) -> None:
        """Run the process on the given node ID.
        """
        start_run_time = time.time()
        self.node_id = node_id
        pl_obj = self.pl_obj
        node = pl_obj.level(id = node_id, action = self.action)
        self.node = node
        self.get_input_vrs()        
        
        node_info = node.get_node_info()
        run_msg = f"Running {node.name} ({node.id})."
        print(run_msg)
        is_batch = pl_obj.batch is not None
        assert is_batch == False
        self.compute_and_assign_outputs(self.inputs, pl_obj, node_info, is_batch)

        self.action.commit = True
        self.action.exec = True
        self.action.execute(return_conn = False)        

        done_run_time = time.time()
        done_msg = f"Running {node.name} ({node.id}) took {round(done_run_time - start_run_time, 3)} seconds."
        print(done_msg)
        
    def check_if_run_node(self, node_id: str) -> bool:
        """Check whether to run the Process on the given node ID. If False, skip. If True, run.
        """
        self.node_id = node_id
        
        for input in self.pl_obj.inputs.values():
            if input.vr._value.exit_code != 0:
                raise ValueError(f"Input VR {input} has exit code {input.vr._value.exit_code}.")
        earliest_output_vr_time = self.get_earliest_output_vr_time()
        latest_input_vr_time = self.get_latest_input_vr_time(self.pl_obj.inputs)
        if latest_input_vr_time < earliest_output_vr_time and not self.force_redo:
            return False
        else:
            return True          

    def get_input_vrs(self) -> dict:
        """Load the input variables.
        """
        self.inputs = {}
        node_lineage = self.node.get_node_lineage()
        for vr_name_in_code, input in self.pl_obj.inputs.items():
            value = self.node.get(input = input, action=self.action, node_lineage=node_lineage,  process=input.pr)
            self.inputs[vr_name_in_code] = value
        
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
        output_vrs_earliest_time = datetime.min.replace(tzinfo=timezone.utc)
        if not hasattr(pr, "outputs"):
            return output_vrs_earliest_time # Logsheet
        output_vr_ids = [output.vr.id for output in pr.outputs.values()]
        sqlquery_raw = "SELECT action_id_num FROM data_values WHERE path_id = ? AND vr_id IN ({})".format(",".join("?" * len(output_vr_ids)))
        sqlquery = sql_order_result(self.action, sqlquery_raw, ["path_id", "vr_id"], single = True, user = True, computer = False) # The data is ordered. The most recent action_id is the first one.
        params = tuple([self.node.id] + output_vr_ids)
        result = cursor.execute(sqlquery, params).fetchall() # The latest action ID
        if len(result) == 0:
            return output_vrs_earliest_time
        else:                
            most_recent_action_id = result[0][0] # Get the latest action_id.
            sqlquery = "SELECT datetime FROM actions WHERE action_id_num = ?"
            params = (most_recent_action_id,)
            result = cursor.execute(sqlquery, params).fetchall()
            output_vrs_earliest_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")

        return output_vrs_earliest_time
    
    def get_latest_input_vr_time(self, inputs: list) -> datetime:
        """Get the datetime when the last input VR was modified.

        Returns:
            datetime: _description_
        """
        from ResearchOS.variable import Variable
        cursor = self.action.conn.cursor()
        # Check if the values for all the input variables are up to date. If so, skip this node.        
        input_vrs_latest_time = datetime.min.replace(tzinfo=timezone.utc)
        for vr_name_in_code, input in inputs.items():
            if hasattr(input.parent_ro, "import_file_vr_name") and vr_name_in_code == input.parent_ro.import_file_vr_name:
                continue # Special case. Skip the import file variable.

            vr = input.vr

            if isinstance(vr, dict) or not isinstance(vr, Variable):
                continue  # Skip DataObject attributes and hard-coded variables (that aren't Variable objects).
            
            # Hard coded. Return when the hard coded value was last set.
            if vr.hard_coded_value is not None:
                sqlquery_raw = "SELECT action_id_num FROM simple_attributes WHERE object_id = ? AND attr_id = ? AND attr_value = ?"
                sqlquery = sql_order_result(self.action, sqlquery_raw, ["object_id", "attr_id", "attr_value"], single = True, user = True, computer = False)
                attr_id = ResearchObjectHandler._get_attr_id("hard_coded_value")
                params = (vr.id, attr_id, json.dumps(vr.hard_coded_value)) # Has to be JSON encoded.
                result = cursor.execute(sqlquery, params).fetchall()
                if len(result) == 0:
                    return datetime.max.replace(tzinfo=timezone.utc) # Force the process to run.
                sqlquery = "SELECT datetime FROM actions WHERE action_id_num = ?"
                params = (result[0][0],)
                result = cursor.execute(sqlquery, params).fetchall()
                new_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")
                if new_time > input_vrs_latest_time:
                    input_vrs_latest_time = new_time
                continue
            
            # Dynamic input variable. Return when the data blob hash was last set for this VR & data object.
            sqlquery_raw = "SELECT action_id_num FROM data_values WHERE vr_id = ?"
            sqlquery = sql_order_result(self.action, sqlquery_raw, ["vr_id"], single = True, user = True, computer = False)
            params = (vr.id,)
            result = cursor.execute(sqlquery, params).fetchall()
            if len(result) == 0:
                return datetime.max.replace(tzinfo=timezone.utc) # Force the process to run.

            latest_action_id = result[0][0]
            sqlquery = "SELECT datetime FROM actions WHERE action_id_num = ?"
            params = (latest_action_id,)
            result = cursor.execute(sqlquery, params).fetchall()
            new_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")
            if new_time > input_vrs_latest_time:
                input_vrs_latest_time = new_time
        return input_vrs_latest_time
    
    def add_matlab_to_path(self, file: str) -> None:
        """Add the MATLAB folder inside the ResearchOS package to the MATLAB path."""
        current_script_dir = os.path.dirname(os.path.abspath(file))
        research_os_dir = os.path.abspath(os.path.join(current_script_dir, '..',))
        matlab_folder = os.path.join(research_os_dir, "matlab")
        if self.matlab_eng is None:
            raise ValueError("MATLAB engine not loaded. Run `pip list` to check whether the MATLAB Engine API is installed. Is MATLAB itself installed?")
        self.matlab_eng.addpath(matlab_folder)    