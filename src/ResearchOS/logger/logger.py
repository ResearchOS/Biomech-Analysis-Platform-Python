# Created following: https://youtu.be/9L77QExPmI0?si=iVuQfyaXmWa5MM-q

import logging.config
import json, os, sys

logger = logging.getLogger("ResearchOS")

def setup_logging():
    config_path = os.sep.join(["ResearchOS","logger","logger_config_2_stderr_file.json"])
    abs_paths = [path + os.sep + config_path for path in sys.path]
    print("PATH: ", abs_paths)
    for path in abs_paths:
        if os.path.exists(path):
            config_path = path
            break
    print(config_path)

    with open(config_path, "r") as f:
        logging_config = json.load(f)    
    for handler_name in logging_config["loggers"]["root"]["handlers"]:
        handler = logging_config["handlers"][handler_name]
        if "filename" in handler:
            filename = handler["filename"]
            parent_dir = os.path.dirname(filename)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
    logging.config.dictConfig(config=logging_config)

setup_logging()