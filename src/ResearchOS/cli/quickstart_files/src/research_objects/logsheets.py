import ResearchOS as ros
from paths import LOGSHEET_PATH

lg = ros.Logsheet(id = "LG1")
lg.path = LOGSHEET_PATH
lg.num_header_rows = 1
lg.headers = []