# Created following: https://youtu.be/9L77QExPmI0?si=iVuQfyaXmWa5MM-q

import logging.config
import json, os

logger = logging.getLogger("ResearchOS")

def setup_logging():
    config_path = "src\ResearchOS\logger\logger_config_2_stderr_file.json"
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

def main():
    setup_logging()
    logger.debug("This is a debug message", extra={"user": "John Doe"})
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    try:
        1/0
    except ZeroDivisionError:
        logger.exception("This is an exception message")
    
main()