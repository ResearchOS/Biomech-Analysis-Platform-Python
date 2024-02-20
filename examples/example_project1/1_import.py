import ResearchOS as ros

ss = ros.Subset(id = "SS1")

# Initialize the variables.
ds = ros.Dataset(id = "DS1")
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
