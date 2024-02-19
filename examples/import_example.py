import ResearchOS as ros
from derivative import derivative
from importer import import_example

## DELETE THE DATABASE FILE AND RUN DB_INITIALIZER FIRST.

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

tr1 = ros.Trial(id = "TR1")
vr_trial = ros.Variable(id = "VR1", name = "test")
tr1.set_value(vr_trial, 1)
a = tr1.test

vr_data_path = ros.Variable(id = "VR0", name = "raw data path", level = ros.Dataset)
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