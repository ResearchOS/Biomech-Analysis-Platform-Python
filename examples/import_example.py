import ResearchOS as ros
from derivative import derivative
from importer import import_example

ds = ros.Dataset(id = "DS1")

ds.schema = {
    ros.Subject: {
        ros.Trial: {}
    }
}
ds.dataset_path = "examples/data"
# ds.addresses = ""

lg = ros.Logsheet(id = "LG1")
lg.path = "Spr23TWW_OA_AllSubjects_032323.csv"

incomplete_headers = [
    ("Date", str),
    ("Subject_Codename", str),
    ("Cohort", str),
    ("Age", int),
    ("Gender", str),
    ("FPs_Used", str),
    ("Visit_Number", int),
    ("Trial_Name_Number", str),
    ("Trial_Type_Task", str),
    ("Side_Of_Interest", str),
    ("Perfect_Trial", int),
    ("Subject_Comments", str),
    ("Researcher_Comments", str),
    ("Motive_Initial_Frame", int),
    ("Motive_Final_Frame", int)
]
headers = []
for i in range(0, 15):
    vr = ros.Variable(id = f"VR{i}")
    headers.append((incomplete_headers[i][0], incomplete_headers[i][1], vr.id))
lg.headers = headers
lg.num_header_rows = 3
lg.class_column_names = []
ss = ros.Subset(id = "SS1")
lg.subset_id = ss.id
lg.read_logsheet() # Puts addresses in the dataset object.

vr_data_path = ros.Variable(id = "VRP", name = "raw data path", level = ros.Dataset)
vr1 = ros.Variable(id = "VR1", name = "raw mocap data", level = ros.Trial)
vr2 = ros.Variable(id = "VR2", name = "sampling rate", level = ros.Trial)
# TODO: Show hard-coded??

# Create & set up the Process object to import the data.
importPR = ros.Process(id = "PR1", name = "import")
importPR.method = import_example
importPR.level = ros.Trial
importPR.input_vr = vr_data_path.id
importPR.output_vr = vr1.id
importPR.subset_id = ss.id
importPR.run_method()

# Create & set up the Process object to compute the derivative.
derivPR = ros.Process(id = "PR1", name = "deriv mocap data")
derivPR.method = derivative
derivPR.level = ros.Trial
derivPR.input_vr = ""
derivPR.output_vr = ""
derivPR.subset_id = ss.id
derivPR.run_method()

derivPR = ros.Process(id = "PR1", name = "deriv mocap data",
            method = derivative, level = ros.Trial, input_vr = "", output_vr = "", subset_id = ss.id)
derivPR.run_method()