import time

import networkx as nx

from ResearchOS.action import Action
from ResearchOS.process_runner import ProcessRunner
from ResearchOS.plot_runner import PlotRunner

def run(self, module_file_path: str, force_redo: bool = False, action: Action = None, return_conn = False):
    """Runs the process."""
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.plot import Plot
    start_time = time.time()
    start_msg = f"Running {self.mfunc_name} on {self.level.__name__}s."
    print(start_msg)
    if action is None:
        action = Action(name = start_msg)
    if self.id.startswith(Process.prefix):
        code_runner = ProcessRunner()  
    elif self.id.startswith(Plot.prefix):
        code_runner = PlotRunner()      
    batches_dict_to_run, all_batches_graph, G, pool = code_runner.prep_for_run(self, action, force_redo)
    curr_batch_graph = nx.MultiDiGraph()
    code_runner.add_matlab_to_path(module_file_path)
    for batch_id, batch_value in batches_dict_to_run.items():
        if self.batch is not None:
            curr_batch_graph = nx.MultiDiGraph(all_batches_graph.subgraph([batch_id] + list(nx.descendants(all_batches_graph, batch_id))))
        code_runner.run_batch(batch_id, batch_value, G, curr_batch_graph)

    if code_runner.matlab_loaded and self.is_matlab:
        ProcessRunner.matlab_eng.rmpath(self.mfolder)
        
    if hasattr(self, "outputs"):
        for vr_name, output in self.outputs.items():
            print(f"Saved VR {vr_name} (VR: {output['main']['vr']}).")
    
    action.add_sql_query(None, "run_history_insert", (action.id_num, self.id))
    self.__setattr__("up_to_date", True, action=action, exec=False)

    action.commit = True
    action.exec = True
    action.execute(return_conn=return_conn)

    self.up_to_date = True # Uses a separate action.

    print(f"Finished running {self.id} on {self.level.__name__}s in {time.time() - start_time} seconds.")