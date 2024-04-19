import time

import networkx as nx

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action
from ResearchOS.plot_runner import PlotRunner
from ResearchOS.vr_handler import VRHandler
from ResearchOS.build_pl import make_all_edges
from ResearchOS.sql.sql_runner import sql_order_result

all_default_attrs = {}
# For MATLAB
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

# Main attributes
all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["inputs"] = {}
all_default_attrs["outputs"] = {}
all_default_attrs["subset"] = None

# For batching
all_default_attrs["batch"] = None

computer_specific_attr_names = ["mfolder"]

class Plot(PipelineObject):
    
    prefix = "PL"

    def __init__(self, is_matlab: bool = all_default_attrs["is_matlab"],
                 mfolder: str = all_default_attrs["mfolder"],
                 mfunc_name: str = all_default_attrs["mfunc_name"],
                 method: str = all_default_attrs["method"],
                 level: str = all_default_attrs["level"],
                 inputs: dict = all_default_attrs["inputs"],
                 subset: str = all_default_attrs["subset"],
                 batch: str = all_default_attrs["batch"],
                 **kwargs):
        if self._initialized:
            return
        self.is_matlab = is_matlab
        self.mfolder = mfolder
        self.mfunc_name = mfunc_name
        self.method = method
        self.level = level
        self.inputs = inputs
        self.subset = subset
        self.batch = batch
        super().__init__(**kwargs)

    def save_inputs(self, inputs: dict, action: Action) -> None:
        """Saving the input variables. is done in the input class."""
        pass
        
    def load_inputs(self, action: Action) -> dict:
        """Load the input variables."""
        from ResearchOS.Bridges.input import Input
        sqlquery_raw = "SELECT id, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value, ro_id, vr_name_in_code, show FROM inputs_outputs WHERE is_input = 1 AND ro_id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["id", "ro_id"], single = True, user = True, computer = False)
        params = (self.id,)
        result = action.conn.cursor().execute(sqlquery, params).fetchall()
        inputs = {}
        for row in result:
            input = Input(id=row[0], action=action)            
            inputs[input.vr_name_in_code] = input
        return inputs

    def save_outputs(self, outputs: dict, action: Action) -> None:
        """Saving the output variables. is done in the output class."""
        pass

    def load_outputs(self, action: Action) -> dict:
        """Nothing here because Plots don't have outputs."""
        pass
    
    def set_inputs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict.
        Edges are created here."""
        standardized_kwargs = VRHandler.standardize_inputs(self, kwargs)
        # self.__setattr__("inputs", standardized_kwargs)
        self.__dict__["inputs"] = standardized_kwargs
        make_all_edges(self)

    def set_outputs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict.
        Edges are NOT created here."""
        standardized_kwargs = VRHandler.standardize_outputs(self, kwargs)
        # self.__setattr__("outputs", standardized_kwargs)  
        self.__dict__["outputs"] = standardized_kwargs  
    
    def run(self, action: Action = None, force_redo: bool = False, return_conn: bool = True) -> None:
        """Execute the attached method.

        Args:
            force_redo (bool, optional): _description_. Defaults to False.
        """
        start_time = time.time()
        start_msg = f"Running {self.mfunc_name} on {self.level.__name__}s."
        print(start_msg)
        if action is None:
            action = Action(name = start_msg)
        plot_runner = PlotRunner()        
        batches_dict_to_run, all_batches_graph, G, pool = plot_runner.prep_for_run(self, action, force_redo)
        plot_runner.add_matlab_to_path(__file__)
        curr_batch_graph = nx.MultiDiGraph()
        for batch_id, batch_value in batches_dict_to_run.items():
            if self.batch is not None:
                curr_batch_graph = nx.MultiDiGraph(all_batches_graph.subgraph([batch_id] + list(nx.descendants(all_batches_graph, batch_id))))
            plot_runner.run_batch(batch_id, batch_value, G, curr_batch_graph)

        if plot_runner.matlab_loaded and self.is_matlab:
            PlotRunner.matlab_eng.rmpath(self.mfolder)

        action.add_sql_query(None, "run_history_insert", (action.id_num, self.id))

        action.commit = True
        action.exec = True
        action.execute(return_conn=return_conn)

        print(f"Finished running {self.id} on {self.level.__name__}s in {time.time() - start_time} seconds.")