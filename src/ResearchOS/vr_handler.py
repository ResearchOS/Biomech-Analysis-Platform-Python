from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet   
    source_type = Union[Process, Logsheet]
    from ResearchOS.research_object import ResearchObject

from ResearchOS.variable import Variable
from ResearchOS.Bridges.input import Input
from ResearchOS.Bridges.output import Output
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.Bridges.input_types import ImportFile, DynamicMain
from ResearchOS.Bridges.port import Port

class VRHandler():
    
    @staticmethod
    def standardize_inputs(parent_ro: "ResearchObject", all_inputs: Union[Input, dict], action: Action = None) -> dict:
        """Standardize the inputs to be of type Input."""
        return_conn = False
        if action is None:
            action = Action(name = "Set_Inputs")
            return_conn = True                

        import_value_default = "import_file_vr_name"       

        # 2. Create/load all of the Inputs
        standardized = {}
        for key, dict_input in all_inputs.items():          

            pr = None
            if isinstance(dict_input, Variable):
                if dict_input.hard_coded_value is None:
                    pr = Input.set_source_pr(parent_ro, dict_input, key)
                    input = Input(vr=dict_input, pr=pr, action=action) # Don't put DynamicMain in database yet because PR may need to be set.
                else:
                    input = Input(value=dict_input.hard_coded_value, action=action, parent_ro=parent_ro, vr_name_in_code=key)
            elif not isinstance(dict_input, Input):
                input = Input(value=dict_input, action=action, parent_ro=parent_ro, vr_name_in_code=key) # Directly hard-coded value. May be a DataObject attribute.
            else:
                input = dict_input

            # 1. import file vr name        
            if hasattr(parent_ro, "import_file_vr_name") and key==parent_ro.import_file_vr_name:
                input.put_value = ImportFile(import_value_default)

            if isinstance(input.put_value, DynamicMain) and input.put_value.lookup_vr.vr is not None and input.put_value.lookup_vr.pr is None:
                input.put_value.lookup_vr.pr = Input.set_source_pr(parent_ro, input.put_value.lookup_vr.vr)    
                input.lookup_pr = input.put_value.lookup_vr.pr   

            if input.parent_ro is None or input.vr_name_in_code is None:
                if input.parent_ro is None:
                    input.parent_ro = parent_ro
                if input.vr_name_in_code is None:
                    input.vr_name_in_code = key
                del input.id
                input.action = action
                input = Input(**input.__dict__)

            standardized[key] = input
                    
        if return_conn:
            action.commit = True
            action.execute()

        return standardized
    
    @staticmethod
    def standardize_outputs(parent_ro: "ResearchObject", all_outputs: Union[Output, dict], action: Action = None) -> dict:
        """Standardize the outputs to be of type Output."""
        return_conn = False
        if action is None:
            action = Action(name = "Set_Inputs")
            return_conn = True                

        # 2. Create/load all of the Outputs
        standardized = {}
        for key, dict_output in all_outputs.items():

            if isinstance(dict_output, Variable):
                output = Output(vr=dict_output, pr=parent_ro, action=action, parent_ro=parent_ro, vr_name_in_code=key)
            else:
                output = dict_output
            if output.parent_ro is None or output.vr_name_in_code is None:
                if output.parent_ro is None:
                    output.parent_ro = parent_ro
                if output.vr_name_in_code is None:
                    output.vr_name_in_code = key
                del output.id
                output = Output(**output.__dict__)

            if output.parent_ro is None:
                output.parent_ro = parent_ro
            if output.vr_name_in_code is None:
                output.vr_name_in_code = key

            standardized[key] = output
        
        if return_conn:
            action.commit = True
            action.execute()
        
        return standardized
    
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