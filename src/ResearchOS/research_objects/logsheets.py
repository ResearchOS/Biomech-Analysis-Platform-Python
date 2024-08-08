import ResearchOS as ros

import research_objects.variables as vr
from research_objects.data_objects import Subject, Trial, Condition
from paths import LOGSHEET_PATH


# Initialize the logsheet.
main_lg = ros.Logsheet(id = "LG1")
main_lg.path = LOGSHEET_PATH

headers = [
    ("Date", str, Subject, vr.date),
    ("Subject_Codename", str, Subject, vr.codename),
    ("Height", str, Subject, vr.height),
    ("Body_Weight", int, Trial, vr.subject_mass),
    ("Gender", str, Subject, vr.gender),
    ("FPs_Used", str, Subject, vr.fps_used),
    ("Mocap_Frame_Rate", int, Trial, vr.mocap_frame_rate),
    ("FP_Frame_Rate", int, Trial, vr.fp_frame_rate),
    ("Visit_Number", int, Subject, vr.visit_num),
    ("Trial_Name_Number", str, Trial, vr.trial_name),
    ("Trial_Type_Task", str, Condition, vr.trial_type),
    ("Visit_Number", int, Subject, vr.visit_num),
    ("Static_Cal_Trial_Name", str, Trial, vr.static_cal_trial_name),
    ("Side_Of_Interest", str, Trial, vr.side_of_interest),
    ("Perfect_Trial", int, Trial, vr.perfect_trial),
    ("Collection_Phase", str, Trial, vr.collection_phase),
    ("Secondary_Measures", str, Trial, vr.secondary_measures),
    ("Subject_Comments", str, Trial, vr.subject_comments),
    ("Researcher_Comments", str, Trial, vr.researcher_comments),
    ("Motive_Marker_ID_Comments", str, Trial, vr.motive_marker_id_comments),
    ("Motive_Initial_Frame", int, Trial, vr.motive_initial_frame),
    ("Motive_Final_Frame", int, Trial, vr.motive_final_frame)
]
main_lg.headers = headers
main_lg.num_header_rows = 3
main_lg.class_column_names = {
    "Subject_Codename": Subject,
    "Trial_Name_Number": Trial,
    "Trial_Type_Task": Condition
}