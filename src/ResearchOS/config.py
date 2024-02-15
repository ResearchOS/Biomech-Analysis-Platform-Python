# Purpose: Configuration files for the application

# How this works:
# 1. The Config class is instantiated.
# 2. The "immutable" config is loaded into memory.
# 3. The "mutable" config is loaded into memory.

# 1. Can change the "mutable" config, but not the "immutable" config.

from typing import Any
import json, os, copy

config_path = os.path.join(os.path.dirname(__file__), "config/config.json")
immutable_config_path = os.path.join(os.path.dirname(__file__), "config/immutable_config.json")

class Config():
    
    def __init__(self) -> None:
        self.__dict__["_config_path"] = config_path
        self.__dict__["_immutable_config_path"] = immutable_config_path
        self.load_config(self._config_path, None)
        self.load_config(self._immutable_config_path, "immutable")

    def load_config(self, config_path: str, key: str) -> None:
        """Load all of the attributes from the config file."""        
        with open(config_path, "r") as f:
            attrs = json.load(f)
        if key is not None:
            key_dict = {}
            key_dict[key] = attrs
            self.__dict__.update(key_dict)
        else:
            self.__dict__.update(attrs)

    def save_config(self, config_path: str) -> None:
        """Save all of the attributes to the config file."""
        attrs = copy.deepcopy(self.__dict__)
        del attrs["immutable"]
        del attrs["_config_path"]
        del attrs["_immutable_config_path"]
        with open(config_path, "w") as f:
            json.dump(attrs, f, indent = 4)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set the attribute and save the config file."""
        self.__dict__[name] = value
        self.save_config(self._config_path)
