"""A Python port of my data analysis pipeline."""

from src.ResearchOS.PipelineObjects.project import Project
from src.ResearchOS.PipelineObjects.analysis import Analysis
from src.ResearchOS.PipelineObjects.logsheet import Logsheet
from src.ResearchOS.DataObjects.dataset import Dataset
from src.ResearchOS.DataObjects.subject import Subject
from src.ResearchOS.DataObjects.visit import Visit
from src.ResearchOS.DataObjects.trial import Trial
from src.ResearchOS.DataObjects.phase import Phase
from src.ResearchOS.PipelineObjects.subset import Subset
from src.ResearchOS.PipelineObjects.process import Process
from src.ResearchOS.variable import Variable

pj= Project(id = "PJE55E00_0CE")  # Create a new project and analysis
an = Analysis(id = "AN1B61E9_5D4")  # Create a new analysis
pj.current_analysis_id = an.id  # Set the current analysis for the project
lg = Logsheet(id = "LGB4002C_900")  # Create a new logsheet
an.current_logsheet_id = lg.id  # Set the current logsheet for the analysis

ds = Dataset(id = "DS4E22F9_437")  # Create a new dataset
pj.current_dataset_id = ds.id  # Set the current dataset for the project

path = "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Biomech-Analysis-Platform-Python"
pj.project_path = path  # Set the project path
ds.dataset_path = path  # Set the dataset path

lg.logsheet_path = path  # Set the logsheet path
lg.logsheet_headers = []  # Set the logsheet headers
lg.class_column_names = {}  # Set the class column names
lg.num_header_rows = 3  # Set the number of header rows

ds.schema = [Dataset, Subject, Visit, Trial, Phase]  # Set the dataset schema

vrs = lg.read_logsheet()  # Read the logsheet
print(vrs)  # Print the variables so I know which ones were created.

def method():
    pass

ss_slg = Subset(id = "SS6BC411_7E1")
ss_slg.level = Trial
ss_slg.conditions["and"]["condition1"] = ["VR5C539C_608", "==", "test"]
ss_slg.conditions["and"]["condition2"] = ["VR5C539C_608", "==", "test"]
ss_slg.conditions["and"]["condition3"] = ["VR5C539C_608", "==", "test"]

pr_import = Process(id = "PR640E80_AF2")
pr_import.level = Process
pr_import.method = method
pr_import.add_subset_id(ss_slg.id)
vr_fps_used = Variable(id = "VR5C539C_608")
vr_fps_used.hard_coded_value = "test"
vr_abs_fps_used = vr_fps_used.get_abstract_object()
vr_abs_fps_used.is_hard_coded = True
pr_import.add_input_variable_id(id = vr_fps_used.id, name_in_code = "fps_used")

vr_mocap_data = Variable(id = "VR613E29_FFA")
pr_import.add_output_variable_id(id = vr_mocap_data.id, name_in_code = "mocap_data")
pr_import.run_method()