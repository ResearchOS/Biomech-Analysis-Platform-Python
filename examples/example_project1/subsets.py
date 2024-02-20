import copy

all_trials_OA = {
    "and": [
        ["VR2", "==", "OA"],
        ["VR8", "in", ["Straight Line Gait", "TWW Pre-planned", "TWW Late-cued","Static Calibration"]],
        ["VR9", "in", ["L", None]],
        ["VR10", "==", 1],        
        ["VR12", "not contains", "Practice"] # NEEDS FIXING FOR A "CONTAINS"
    ]
}

all_trials_YA = copy.deepcopy(all_trials_OA)
all_trials_YA["and"][0][2] = "YA"