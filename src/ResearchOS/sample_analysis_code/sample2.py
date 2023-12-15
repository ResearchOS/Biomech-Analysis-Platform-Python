from pipeline_objects.process import Process
from data_objects.dataset import Dataset

dataset = Dataset.get_current()
subject = dataset.subjects(name = "S1")
trial = subject.trials(name = "T1")
phase = trial.phases(name = "P1")

phases = Dataset(specify_trials = {})

pr = Process.load(id = "")
func_name = ""
pr.assign_func(func_name)
pr.assign_inputs(vr_id = [], name_in_code = [])
pr.assign_outputs(vr_id = [], name_in_code = [])
pr.run(phases)