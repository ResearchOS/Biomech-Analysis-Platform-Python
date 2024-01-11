"""A Python port of my data analysis pipeline."""

# Set Python path
import sys
sys.path.append("/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Biomech-Analysis-Platform-Python/src")
import ResearchOS as ros

ros.DBInitializer(ros.Config.db_file)

# from src.ResearchOS.SQL.database_init import DBInitializer as DBInit
# from src.ResearchOS.config import ProdConfig
# db = DBInit(ProdConfig.db_file)
# del db

### PROBLEM: I need to be able to create a current user, but that requires an Action, which requires a current user.

us = ros.User("US000000_001", name = "Mitchell Tillman", current_user = True)
ros.User.set_current_user_object_id(us.id)
pj= ros.Project(id = "PJE55E00_0CE")  # Create a new project and analysis
us.current_project_id = pj.id  # Set the current project for the user
an = ros.Analysis(id = "AN1B61E9_5D4")  # Create a new analysis
pj.test = None
pj.current_analysis_id = an.id  # Set the current analysis for the project
lg = ros.Logsheet(id = "LGB4002C_900")  # Create a new logsheet
an.current_logsheet_id = lg.id  # Set the current logsheet for the analysis

ds = ros.Dataset(id = "DS4E22F9_437")  # Create a new dataset
pj.current_dataset_id = ds.id  # Set the current dataset for the project

path = "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Biomech-Analysis-Platform-Python"
pj.project_path = path  # Set the project path
ds.dataset_path = path  # Set the dataset path

lg.logsheet_path = path  # Set the logsheet path
lg.logsheet_headers = []  # Set the logsheet headers
lg.class_column_names = {}  # Set the class column names
lg.num_header_rows = 3  # Set the number of header rows

ds.schema = [ros.Dataset, ros.Subject,ros. Visit, ros.Trial, ros.Phase]  # Set the dataset schema

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