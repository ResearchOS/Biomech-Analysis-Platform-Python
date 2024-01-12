# Purpose: Configuration file for the application

import json, os, sys

class GeneralConfig():

    general_config_file: str = "config/general_config.json"
    general_attr_names = [
        "abstract_id_len",
        "instance_id_len"
    ]

    def init_config(self, config_file: str) -> None:
        """Initialize the config file."""
        
        attrs = json.load(open(config_file, "r"))
        self.__dict__.update({"env_attr_names": list(attrs.keys())})
        self.__dict__.update({"config_file": config_file})
        self.__dict__.update(attrs)

    def __init__(self, config_file: str = general_config_file) -> None:
        """Load all of the attributes from the config file."""
        self.__dict__.update({"general_config_file": config_file})
        attrs = json.load(open(config_file, "r"))          
        self.__dict__.update({"general_attr_names": list(attrs.keys())})
        self.__dict__.update(attrs)

    def __setattr__(self, name: str, value: str) -> None:
        """Set the attribute and save it to the config file."""        
        if name in self.general_attr_names:
            file = self.general_config_file
        elif name in self.env_attr_names:
            file = self.config_file

        attrs = json.load(open(file, "r"))
        attrs[name] = value
        json.dump(attrs, open(file, "w"), indent = 4)
        setattr(self, name, value)

class ProdConfig(GeneralConfig):
    config_file = "config/prod_config.json"
    def __init__(self, config_file: str = config_file) -> None:
        """Initialize the config file."""
        self.init_config(config_file = config_file)        

class DevConfig(GeneralConfig):
    # When would I use this one?
    config_file = "src/ResearchOS/config/dev_config.json"
    def __init__(self, config_file: str = config_file) -> None:
        """Initialize the config file."""
        self.init_config(config_file = config_file)        

class TestConfig(GeneralConfig):    
    config_file = "src/ResearchOS/config/test_config.json"    
    def __init__(self, config_file: str = config_file) -> None:
        """Initialize the config file."""
        self.init_config(config_file = config_file)    

class Config():

    def __init__(self):
        if os.environ.get("ENV") == "dev":
            config = DevConfig()
        elif os.environ.get("ENV") == "test":
            config = TestConfig()
        elif os.environ.get("ENV") == "prod":
            config = ProdConfig()
        self.__dict__.update(config.__dict__)
        self.__dict__.update({"config_file": config.config_file})
        self.__dict__.update({"db_file": config.db_file})

if __name__=="__main__":
    os.environ["ENV"] = "test"
    config = Config()
    print(config.abstract_id_len)
    config.abstract_id_len = 10
    print(config.abstract_id_len)