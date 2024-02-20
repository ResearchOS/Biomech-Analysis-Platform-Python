import ResearchOS as ros
from derivative import derivative
from importer import import_example
from ResearchOS.config import Config

## DELETE THE DATABASE FILE AND RUN DB_INITIALIZER FIRST.
db_file = "dev_database.db"
config = Config()
config.db_file = db_file
ros.DBInitializer()

# Initialize the dataset
ds = ros.Dataset(id = "DS1")
ds.schema = [
    [ros.Dataset, ros.Subject],
    [ros.Subject, ros.Trial]
]
ds.dataset_path = "examples\data"

# Initialize the logsheet.
lg = ros.Logsheet(id = "LG1")
lg.path = "Spr23TWW_OA_AllSubjects_032323_TEST.csv"

incomplete_headers = [
    ("Date", str, ros.Subject),
    ("Subject_Codename", str, ros.Subject),
    ("Cohort", str, ros.Subject),
    ("Age", int, ros.Subject),
    ("Gender", str, ros.Subject),
    ("FPs_Used", str, ros.Subject),
    ("Visit_Number", int, ros.Subject),
    ("Trial_Name_Number", str, ros.Trial),
    ("Trial_Type_Task", str, ros.Trial),
    ("Side_Of_Interest", str, ros.Trial),
    ("Perfect_Trial", int, ros.Trial),
    ("Subject_Comments", str, ros.Trial),
    ("Researcher_Comments", str, ros.Trial),
    ("Motive_Initial_Frame", int, ros.Trial),
    ("Motive_Final_Frame", int, ros.Trial)
]
headers = []
for i in range(0, len(incomplete_headers)):
    vr = ros.Variable(id = f"VR{i}")
    headers.append((incomplete_headers[i][0], incomplete_headers[i][1], incomplete_headers[i][2], vr.id))
lg.headers = headers
lg.num_header_rows = 3
lg.class_column_names = {
    "Subject_Codename": ros.Subject,
    "Trial_Name_Number": ros.Trial
}
ss = ros.Subset(id = "SS1")
lg.subset_id = ss.id
lg.read_logsheet() # Puts addresses in the dataset object.

# Initialize the variables.
dataset_path_vr = ros.Variable(id = "VR15", name = "dataset_path", level = ros.Dataset, hard_coded_value = ds.dataset_path)
vr1 = ros.Variable(id = "VR16", name = "raw_mocap_data", level = ros.Trial)
vr2 = ros.Variable(id = "VR17", name = "sampling_rate", level = ros.Trial)

conditions = {
    "and": [
        ["VR2", "==", "OA"],
        ["VR8", "in", ["Straight Line Gait", "TWW Pre-planned", "TWW Late-cued","Static Calibration"]],
        ["VR9", "in", ["L", None]],
        ["VR10", "==", 1],        
        ["VR12", "not contains", "Practice"] # NEEDS FIXING FOR A "CONTAINS"
    ]
}
ss.conditions = conditions

# Create & set up the Process object to import the data.
importPR = ros.Process(id = "PR1", name = "import")
importPR.level = ros.Trial
importPR.is_matlab = True
importPR.set_input_vrs(path = dataset_path_vr)
importPR.set_output_vrs(mocap_data = vr1, force_data = vr2)
importPR.subset_id = ss.id
importPR.mfolder = "C:\\Users\\Mitchell\\Desktop\\Matlab Code\\GitRepos\\PGUI_CommonPath\\Code\\Process_Functions_Copy_For_Python"
importPR.mfunc_name = "test"
importPR.run() # OA
conditions["and"][0][2] = "YA"
ss.conditions = conditions
importPR.run() # YA

# Create & set up the Process object to compute the derivative.
# derivPR = ros.Process(id = "PR1", name = "deriv mocap data")
# derivPR.method = derivative
# derivPR.level = ros.Trial
# derivPR.input_vr = ""
# derivPR.output_vr = ""
# derivPR.subset_id = ss.id
# derivPR.run_method()

# derivPR = ros.Process(id = "PR1", name = "deriv mocap data",
#             method = derivative, level = ros.Trial, input_vr = "", output_vr = "", subset_id = ss.id)
# derivPR.run_method()
