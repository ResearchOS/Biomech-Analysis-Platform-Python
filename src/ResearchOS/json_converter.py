from typing import Any, TYPE_CHECKING, Callable
import json, sys, os
import importlib

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.PipelineObjects.subset import Subset


from ResearchOS.action import Action


class JSONConverter():
    """First looks in the ResearchObject for a method to convert to/from JSON. If not found, looks for a method in this class.
    If still not found, uses json.dumps/loads."""

    def to_json(robj: "ResearchObject", name: str, input: Any, action: Action) -> str:
        to_json_method = getattr(robj, f"to_json_{name}", None)
        method_from_ro = True
        if to_json_method is None:
            to_json_method = getattr(JSONConverter, f"to_json_{name}", None)
            method_from_ro = False
        if to_json_method is not None:
            if not method_from_ro:
                return to_json_method(robj, input, action)
            else:
                return to_json_method(input, action)
        else:
            return json.dumps(input)

    def from_json(robj: "ResearchObject", name: str, input: str, action: Action) -> Any:
        from_json_method = getattr(robj, f"from_json_{name}", None)
        method_from_ro = True
        if from_json_method is None:            
            from_json_method = getattr(JSONConverter, f"from_json_{name}", None)
            method_from_ro = False
        if from_json_method is not None:
            if not method_from_ro:
                return from_json_method(robj, input, action)
            else:
                return from_json_method(input, action)
        else:
            return json.loads(input)
        
    @staticmethod
    def from_json_inputs(self, inputs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of input variables."""
        from ResearchOS.Bridges.input import Input
        from ResearchOS.Bridges.inlet import Inlet
        serializable_dict = json.loads(inputs)
        tmp_dict = {}
        for key in serializable_dict:
            inlet = Inlet(self, key, action)                        
            input = Input(id=serializable_dict[key], action=action, let=inlet)
            inlet.add_put(input, action, do_insert=False)
            tmp_dict[key] = inlet
        return tmp_dict
    
    @staticmethod
    def to_json_inputs(self, inputs: dict, action: Action) -> str:
        """Convert the .inputs attribute of a runnable Pipeline Object to a JSON string.
        The format is a dictionary with the inlet names in code as keys, and the values are the input ID's."""
        tmp_dict = {}
        for key, inlet in inputs.items():
            tmp_dict[key] = None
            if len(inlet.puts) > 0:
                tmp_dict[key] = inlet.puts[0].id                            
        return json.dumps(tmp_dict)
    
    @staticmethod
    def from_json_outputs(self, outputs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of output variables."""
        from ResearchOS.Bridges.output import Output
        from ResearchOS.Bridges.outlet import Outlet
        serializable_dict = json.loads(outputs)
        tmp_dict = {}
        for key in serializable_dict:
            tmp_dict[key] = Outlet(self, key, action)
            tmp_dict[key].add_put(Output(id=serializable_dict[key], action=action), action, do_insert=False)
        return tmp_dict
    
    @staticmethod
    def to_json_outputs(self, outputs: dict, action: Action) -> str:
        """Convert the .outputs attribute of a runnable Pipeline Object to a JSON string.
        The format is a dictionary with the outlet names in code as keys, and the values are the output ID's."""
        tmp_dict = {}
        for key, outlet in outputs.items():
            tmp_dict[key] = None
            if len(outlet.puts) > 0:
                tmp_dict[key] = outlet.puts[0].id                            
        return json.dumps(tmp_dict)
    
    @staticmethod
    def from_json_batch(self, batch: str, action: Action) -> list:
        """Convert a JSON string to a list of batch elements."""
        from ResearchOS.DataObjects.data_object import DataObject
        prefix_list = json.loads(batch)
        if prefix_list is None:
            return None
        data_subclasses = DataObject.__subclasses__()
        return [cls for cls in data_subclasses if cls.prefix in prefix_list]    
    
    @staticmethod
    def to_json_batch(self, batch: list, action: Action) -> str:
        """Convert a list of batch elements to a JSON string."""
        if batch is None:
            return json.dumps(None)
        return json.dumps([cls.prefix for cls in batch])
    
    @staticmethod
    def from_json_level(self, json_level: str, action: Action) -> type:
        """Convert a JSON string to a Process level.
        
        Args:
            self
            level (string) : IDK
            
        Returns:
            the JSON ''level'' as a type"""
        from ResearchOS.DataObjects.data_object import DataObject
        level = json.loads(json_level)
        classes = DataObject.__subclasses__()
        for cls in classes:
            if hasattr(cls, "prefix") and cls.prefix == level:
                return cls

    @staticmethod
    def to_json_level(self, level: type, action: Action) -> str:
        """Convert a Process level to a JSON string."""
        if level is None:
            return json.dumps(None)
        return json.dumps(level.prefix)
    
    @staticmethod
    def to_json_subset(self, subset: "Subset", action: Action) -> str:
        """Convert a Subset to a JSON string."""
        if subset is None:
            return json.dumps(None)
        return json.dumps(subset.id)
    
    @staticmethod
    def from_json_subset(self, json_subset: str, action: Action) -> "Subset":
        """Convert a JSON string to a Subset."""
        from ResearchOS.PipelineObjects.subset import Subset
        subset_id = json.loads(json_subset)
        if subset_id is None:
            return None
        return Subset(id = subset_id, action = action)
    
    @staticmethod
    def from_json_method(self, json_method: str, action: Action) -> Callable:
        """Convert a JSON string to a method.
        
        Args:
            self
            json_method (string) : JSON method to convert as a string
        
        Returns:
            Callable: IDK note add fancy linking thing once you know what a callable
            Returns None if the method name is not found (e.g. if code changed locations or something)"""
        method_name = json.loads(json_method)
        if method_name is None:
            return None
        module_name, *attribute_path = method_name.split(".")
        if module_name not in sys.modules:
            module = importlib.import_module(module_name)
        attribute = module
        for attr in attribute_path:
            attribute = getattr(attribute, attr)
        return attribute

    @staticmethod
    def to_json_method(self, method: Callable, action: Action) -> str:
        """Convert a method to a JSON string.
        
        Args:
            self
            method (Callable) : python object representing code thats about to be run
        
        Returns:
            the method as a JSON string"""
        if method is None:
            return json.dumps(None)
        return json.dumps(method.__module__ + "." + method.__qualname__)