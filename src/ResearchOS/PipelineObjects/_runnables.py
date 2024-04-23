import time

import networkx as nx

from ResearchOS.action import Action
from ResearchOS.process_runner import ProcessRunner

def run(self, module_file_path: str, force_redo: bool = False, action: Action = None, return_conn = False):
    start_time = time.time()
    start_msg = f"Running {self.mfunc_name} on {self.level.__name__}s."
    print(start_msg)
    if action is None:
        action = Action(name = start_msg)
    process_runner = ProcessRunner()        
    batches_dict_to_run, all_batches_graph, G, pool = process_runner.prep_for_run(self, action, force_redo)
    curr_batch_graph = nx.MultiDiGraph()
    process_runner.add_matlab_to_path(module_file_path)
    for batch_id, batch_value in batches_dict_to_run.items():
        if self.batch is not None:
            curr_batch_graph = nx.MultiDiGraph(all_batches_graph.subgraph([batch_id] + list(nx.descendants(all_batches_graph, batch_id))))
        process_runner.run_batch(batch_id, batch_value, G, curr_batch_graph)

    if process_runner.matlab_loaded and self.is_matlab:
        ProcessRunner.matlab_eng.rmpath(self.mfolder)
        
    for vr_name, output in self.outputs.items():
        print(f"Saved VR {vr_name} (VR: {output['main']['vr']}).")

    action.add_sql_query(None, "run_history_insert", (action.id_num, self.id))

    action.commit = True
    action.exec = True
    action.execute(return_conn=return_conn)

    print(f"Finished running {self.id} on {self.level.__name__}s in {time.time() - start_time} seconds.")