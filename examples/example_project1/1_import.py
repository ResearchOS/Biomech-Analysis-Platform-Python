import json, os

import ResearchOS as ros

from subsets import all_trials_OA, all_trials_YA

ss = ros.Subset(id = "SS1")
ss.conditions = all_trials_OA

# Initialize the variables.
ds = ros.Dataset(id = "DS1")
ds.dataset_path = "C:\\Users\\Mitchell\\Desktop\\Matlab Code\\GitRepos\\Spr23-YA-OA-Role-Gait-Phase\\Raw Data Files"
input_vr1 = ros.Variable(id = "VR15", name = "dataset_path", level = ros.Dataset, hard_coded_value = ds.dataset_path)
mocapfpHelper = ros.Variable(id = "VR16", name = "mocapfpHelper", level = ros.Trial)
with open (os.sep.join(["examples", "example_project1", "mocapfpHelper.json"]), "r") as f:
    mocapfpHelper.hard_coded_value = json.load(f)
fpsUsed = ros.Variable(id = "VR5")

# Output variables
fp_cover_thickness = ros.Variable(id = "VR17", name = "FPCoverThickness", level = ros.Trial)
cop_fz_threshold = ros.Variable(id = "VR18", name = "COPFzThreshold", level = ros.Trial)
card_corners = ros.Variable(id = "VR19", name = "cardCorners", level = ros.Trial)
card_forces = ros.Variable(id = "VR20", name = "cardForces", level = ros.Trial)
fp_center = ros.Variable(id = "VR21", name = "fpCenter", level = ros.Trial)
card_moments = ros.Variable(id = "VR22", name = "cardMoments", level = ros.Trial)
card_free_moments = ros.Variable(id = "VR23", name = "cardFreeMoments", level = ros.Trial)
card_cop = ros.Variable(id = "VR24", name = "cardCOP", level = ros.Trial)
fp_ref_frame = ros.Variable(id = "VR25", name = "fpRefFrame", level = ros.Trial)
fp_frame_rate = ros.Variable(id = "VR26", name = "fpFrameRate", level = ros.Trial)
fp_amp_serial_num = ros.Variable(id = "VR27", name = "fpAmpSerialNum", level = ros.Trial)
fp_position = ros.Variable(id = "VR28", name = "fpPosition", level = ros.Trial)
fp_type = ros.Variable(id = "VR29", name = "fpType", level = ros.Trial)
fp_size = ros.Variable(id = "VR30", name = "fpSize", level = ros.Trial)
fp_rot_matrix2_cardinal = ros.Variable(id = "VR31", name = "fpRotMatrix2Cardinal", level = ros.Trial)
comp_cop_fp_logical = ros.Variable(id = "VR32", name = "compCOPFPLogical", level = ros.Trial)
card_mocap_data = ros.Variable(id = "VR33", name = "cardMocapData", level = ros.Trial)
mocap_frame_rate = ros.Variable(id = "VR34", name = "mocapFrameRate", level = ros.Trial)
mocap_ref_frame = ros.Variable(id = "VR35", name = "mocapRefFrame", level = ros.Trial)
mocap_rot_matrix2_cardinal = ros.Variable(id = "VR36", name = "mocapRotMatrix2Cardinal", level = ros.Trial)
seg_marker_names = ros.Variable(id = "VR37", name = "segMarkerNames", level = ros.Trial)

# Create & set up the Process object to import the data.
importPR = ros.Process(id = "PR1", name = "import")
importPR.level = ros.Trial
importPR.is_matlab = True
importPR.set_input_vrs(mocapfpHelper = mocapfpHelper, fpsUsed = fpsUsed, c3dFilePath = input_vr1)
importPR.set_output_vrs(FPCoverThickness = fp_cover_thickness, COPFzThreshold = cop_fz_threshold, cardCorners = card_corners, cardForces = card_forces, fpCenter = fp_center, 
                        cardMoments = card_moments, cardFreeMoments = card_free_moments, cardCOP = card_cop, fpRefFrame = fp_ref_frame, fpFrameRate = fp_frame_rate, fpAmpSerialNum = fp_amp_serial_num,
                        fpPosition = fp_position, fpType = fp_type, fpSize = fp_size, fpRotMatrix2Cardinal = fp_rot_matrix2_cardinal, compCOPFPLogical = comp_cop_fp_logical, cardMocapData = card_mocap_data, 
                        mocapFrameRate = mocap_frame_rate, mocapRefFrame = mocap_ref_frame, mocapRotMatrix2Cardinal = mocap_rot_matrix2_cardinal, segMarkerNames = seg_marker_names)
importPR.subset_id = ss.id
importPR.mfolder = "C:\\Users\\Mitchell\\Desktop\\Matlab Code\\GitRepos\\PGUI_CommonPath\\Code\\Process_Functions_Copy_For_Python"
importPR.mfunc_name = "importMocapFP_Rigid_Bodies_ReadC3D"
importPR.run() # OA
ss.conditions = all_trials_YA
importPR.run() # YA
