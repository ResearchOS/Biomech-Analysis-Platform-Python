from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet   
    source_type = Union[Process, Logsheet]

from ResearchOS.variable import Variable
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.Bridges.input import Input
from ResearchOS.Bridges.output import Output

class VRHandler():

    # @staticmethod
    # def add_slice_to_input_vrs(input_dict: dict):
    #     """To add a slice to a "vr", it must be a ResearchOS Variable object."""
    #     new_dict = {}
    #     for key, input in input_dict.items():
    #         if not isinstance(vr, Variable) and not (isinstance(vr, dict) and list(vr.keys())[0] in DataObject.__subclasses__()):
    #             new_dict[key] = vr # The variable is hard-coded
    #         else:
    #             new_dict[key] = {}
    #             new_dict[key]["vr"] = vr
    #             slice = getattr(vr, "slice", None)
    #             new_dict[key]["slice"] = slice
    #             try:
    #                 del vr.slice
    #             except:
    #                 pass
    #     return new_dict
    
    @staticmethod
    def standardize_inputs(all_inputs: Union[Input, dict]) -> dict:
        """Standardize the inputs to be of type Input."""
        inputs = {}
        for vr_name_in_code, input in all_inputs.items():
            if input is None:
                inputs[vr_name_in_code] = Input()
            elif isinstance(input, dict) and "vr" in input:
                inputs[vr_name_in_code] = Input(**input) # Multiple kwargs were specified.
            elif isinstance(input, (dict, Variable)):
                inputs[vr_name_in_code] = Input(vr = input) # Only a Variable was specified.
            else:
                inputs[vr_name_in_code] = input # Already an Input.
        return inputs
    
    @staticmethod
    def standardize_outputs(all_outputs: Union[Output, dict]) -> dict:
        """Standardize the outputs to be of type Output."""
        outputs = {}
        for vr_name_in_code, output in all_outputs.items():
            if output is None:
                outputs[vr_name_in_code] = Output()
            elif not isinstance(output, Output):
                outputs[vr_name_in_code] = Output(vr = output)
            else:
                outputs[vr_name_in_code] = output
        return outputs
    
    @staticmethod
    def deserialize_input_vr(vr: Union[None, dict]) -> Union[None, "Variable"]:
        """Convert the VR to a Variable object."""
        from ResearchOS.DataObjects.data_object import DataObject
        from ResearchOS.variable import Variable
        dataobject_subclasses = DataObject.__subclasses__()
        if vr is None:
            return vr
        
        vr_value = vr["vr"]
        # DataObject attributes.
        if isinstance(vr_value, dict):
            cls_prefix = [key for key in vr_value.keys()][0]
            attr_name = [value for value in vr_value.values()][0]
            cls = [cls for cls in dataobject_subclasses if cls.prefix == cls_prefix][0]
            return {cls: attr_name}
        
        # Variables
        return {"vr": Variable(id = vr_value), "slice": vr["slice"]}
    
    @staticmethod
    def deserialize_output_vr(vr: Union[None, dict]) -> Union[None, "Variable"]:
        """Convert the VR to a Variable object."""
        from ResearchOS.variable import Variable
        if vr is None:
            return vr
        
        return Variable(id = vr["vr"])

    @staticmethod
    def deserialize_pr(pr: Union[None, str, list]) -> Union[None, "source_type"]:
        """Convert the PR to a Process or Logsheet object."""
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        from ResearchOS.PipelineObjects.process import Process
        if pr is None:
            return pr
        
        # Scalar value
        if not isinstance(pr, list):
            if pr.startswith(Process.prefix):
                return Process(id = pr)
            else:
                return Logsheet(id = pr)
            
        # List of values
        return [Process(id = value) if value.startswith(Process.prefix) else Logsheet(id = value) for value in pr]

    @staticmethod
    def deserialize_lookup_vr(lookup_vr: Union[None, str]) -> Union[None, "Variable"]:
        """Convert the lookup VR to a Variable object."""
        from ResearchOS.variable import Variable
        if lookup_vr is None:
            return lookup_vr
        
        return Variable(id = lookup_vr)
    
    @staticmethod
    def serialize_input_vr(vr: Union[None, "Variable", dict]) -> Union[None, dict]:
        """Convert the VR to a dictionary that can be JSON serialized."""
        if vr is None:
            return vr
        
        if not hasattr(vr, "slice"):
            slice = None
        else:
            slice = vr.slice
        
        # For DataObject attributes.
        if isinstance(vr, dict):
            cls = list(vr.keys())[0]
            attr_name = list(vr.values())[0]
            return {"vr": {cls.prefix: attr_name}, "slice": None}
        
        # Dict format: {"vr": vr.id, "slice": vr.slice}        
        return {"vr": vr.id, "slice": slice}

    @staticmethod
    def serialize_output_vr(vr: Union[None, "Variable"]) -> Union[None, dict]:
        """Convert the VR to a dictionary that can be JSON serialized."""
        if vr is None:
            return vr
        
        return {"vr": vr.id}

    @staticmethod
    def serialize_lookup_vr(lookup_vr: Union[None, "Variable"]) -> Union[None, str]:
        """Convert the lookup VR to a str that can be JSON serialized."""
        if lookup_vr is None:
            return lookup_vr
        
        return lookup_vr.id

    @staticmethod
    def serialize_pr(pr: Union[None, "source_type"]) -> Union[None, str, list]:
        """Convert the PR to a string or list of strings that can be JSON serialized."""
        if pr is None:
            return pr
        
        if not isinstance(pr, list):
            return pr.id
        
        return [value.id for value in pr]