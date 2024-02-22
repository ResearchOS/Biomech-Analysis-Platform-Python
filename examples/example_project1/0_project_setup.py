import ResearchOS as ros

## DELETE THE DATABASE FILE AND RUN DB_INITIALIZER FIRST.

ros.DBInitializer()

# Initialize the dataset
ds = ros.Dataset(id = "DS1")
ds.schema = [
    [ros.Dataset, ros.Subject],
    [ros.Subject, ros.Trial]
]

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