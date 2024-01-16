# Purpose: Configuration files for the application

# How this works:
# 1. There are two "default" config file templates in this project's config folder: dev/prod/test and general.
#   The general config file contains attributes that are common to all environments.
# When the application is run:
# 1. Checks the OS's standard location for the config file's existence. If it exists, it loads it into memory.
# If it does not exist:
# 1. The two appropriate config files are both loaded into memory.
# 2. Saves the config file to the OS's standard location.

import json, os
from pathlib import Path
import platform
from typing import Any

class GeneralConfig():

    default_general_config_file = str(Path("src", "ResearchOS", "config", "general_config.json").resolve())


    def __init__(self) -> None:
        """Load all of the attributes from the config file."""
        # 1. Checks the OS's standard location for the config file's existence. If it exists, it loads it into memory.
        parent_folder = os.path.dirname(self.default_general_config_file)
        # Create the parent folder if it doesn't exist.
        if not os.path.exists(parent_folder):
            os.makedirs(parent_folder)
        # Create the config file if it doesn't exist.        
        if os.path.exists(self.config_file):
            ConfigHandler.load_config(self, self.config_file)
        else:
            # Merge general and env-specific configs, and save them.
            ConfigHandler.load_config(self.default_general_config_file)
            ConfigHandler.load_config(self.default_config_file)
            ConfigHandler.save_config(self.config_file)        

def get_os_config_file_path() -> str:
    """Get the OS's standard location for the config file."""
    if platform.system() == "Darwin":
        home_dir = os.path.expanduser("~")
        base_path = os.path.join(home_dir, "Library", "Application Support", "ResearchOS")
    elif platform.system() == "Windows":
        base_path = os.path.join(os.environ["APPDATA"], "ResearchOS")    
    elif platform.system() == "Linux":
        base_path = os.path.join(os.environ["HOME"], ".config", "ResearchOS")
    return os.path.join(base_path, "config.json")

class ProdConfig(GeneralConfig):
    """Get the config file for the production environment."""
    default_config_file = str(Path("src", "ResearchOS", "config", "prod_config.json").resolve())
    config_file = get_os_config_file_path()

class DevConfig(GeneralConfig):   
    """Get the config file for the development environment.""" 
    default_config_file = str(Path("src", "ResearchOS", "config", "dev_config.json").resolve())
    config_file = str(Path("src", "ResearchOS", "config_no_prod", "config.json").resolve())

class TestConfig(GeneralConfig):   
    """Get the config file for the test environment."""
    default_config_file = str(Path("src","ResearchOS","config","test_config.json").resolve())
    config_file = str(Path("src","ResearchOS","config_no_prod","test_config.json").resolve())     

class Config():

    def __init__(self):
        if os.environ.get("ENV") == "dev":
            config = DevConfig()
        elif os.environ.get("ENV") == "test":
            config = TestConfig()
        elif os.environ.get("ENV") == "prod":
            config = ProdConfig()
        self.__dict__.update({**config.__dict__, **{"config_file": config.config_file}})    

    def __setattr__(self, name, value) -> None:
        """Set the attribute and save the config file."""
        self.__dict__.update({name: value})
        ConfigHandler.save_config(self, self.config_file)

class ConfigHandler():

    def load_config(self: Config, config_file: str) -> None:
        """Load all of the attributes from the config file."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file {config_file} does not exist.")
        attrs = json.load(open(config_file, "r"))
        self.__dict__.update(attrs)

    def save_config(self: Config, config_file: str) -> None:
        """Save all of the attributes to the config file."""
        folder = os.path.dirname(config_file)
        if not os.path.exists(folder):
            os.makedirs(folder)
        tmp_attrs = {**config.__dict__, **vars(type(config))} # Get class variables and instance variables.
        attrs = {}
        for attr in tmp_attrs:
            if attr not in ["default_config_file"] and not attr.startswith("__"):
                attrs[attr] = tmp_attrs[attr]
        try:
            json.dump(attrs, open(config_file, "w"), indent = 4)
        except:
            raise Exception(f"Unable to save config file {config_file}.")

if __name__=="__main__":
    os.environ["ENV"] = "dev"
    config = Config()
    config.test = "test"
    print(config.__dict__)