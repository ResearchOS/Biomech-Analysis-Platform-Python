from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet   
    source_type = Union[Process, Logsheet]
    from ResearchOS.research_object import ResearchObject

from ResearchOS.variable import Variable
from ResearchOS.Bridges.inlet import Inlet
from ResearchOS.Bridges.outlet import Outlet
from ResearchOS.Bridges.input import Input
from ResearchOS.Bridges.output import Output
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result

class VRHandler():
    
    @staticmethod
    def standardize_inputs(parent_ro: "ResearchObject", all_inputs: Union[Input, dict], action: Action = None) -> dict:
        """Standardize the inputs to be of type Input."""
        return_conn = False
        if action is None:
            action = Action(name = "Set_Inputs")
            return_conn = True
        # 1. Create/load all of the Inlets
        inlets_dict = {key: Inlet(parent_ro, key, action) for key in all_inputs.keys()}        

        # See which inputs are already associated with the inlets
        # sqlquery_raw = "SELECT put_id FROM lets_puts WHERE let_id IN ({})".format(", ".join(["?" for _ in inlets_dict]))
        # params = tuple([inlet.id for inlet in inlets_dict.values()])
        # sqlquery = sql_order_result(action, sqlquery_raw, ["put_id", "let_id"], single = True, user = True, computer = False)
        # result = action.conn.cursor().execute(sqlquery, params).fetchall()
        # put_ids = [row[0] for row in result]

        # inputs = [Input.load(id=put_id) for put_id in put_ids]

        # 2. Create/load all of the Inputs        
        for key, input in all_inputs.items():
            inlet = inlets_dict[key]

            found_input = False
            for tmp_input in inputs:
                if found_input:
                    continue
                if isinstance(input, Input):
                    pass
                elif isinstance(input, Variable): 
                    if tmp_input.vr == input:
                        found_input = True
                        input = tmp_input  
                    else:     
                        pass                                                        
                        # input = Input(vr=input, action=action, let=inlet)
                else:
                    if tmp_input.value == input:
                        found_input = True
                        input = tmp_input                        
                    # input = Input(value=input, action=action, let=inlet) # Directly hard-coded value. May be a DataObject attribute.            
            if not found_input:
                pass

            do_insert = True
            if found_input:
                do_insert = input.id not in put_ids
            inlet.add_put(input, action=action, do_insert=do_insert)
        
        if return_conn:
            action.commit = True
            action.execute()
        
        return inlets_dict
    
    @staticmethod
    def standardize_outputs(parent_ro: "ResearchObject", all_outputs: Union[Output, dict], action: Action = None) -> dict:
        """Standardize the outputs to be of type Output."""
        return_conn = False
        if action is None:
            action = Action(name = "Set_Inputs")
            return_conn = True
        # 1. Create/load all of the Outlets
        outlets_dict = {key: Outlet(parent_ro, key, action) for key in all_outputs.keys()}

        # See which outputs are already associated with the outlets
        sqlquery_raw = "SELECT put_id FROM lets_puts WHERE let_id IN ({})".format(", ".join(["?" for _ in outlets_dict]))
        params = tuple([outlet.id for outlet in outlets_dict.values()])
        sqlquery = sql_order_result(action, sqlquery_raw, ["put_id", "let_id"], single = True, user = True, computer = False)
        result = action.conn.cursor().execute(sqlquery, params).fetchall()
        put_ids = [row[0] for row in result]

        # 2. Create/load all of the Outputs
        for key, output in all_outputs.items():
            outlet = outlets_dict[key]

            if isinstance(output, Output):
                pass
            elif isinstance(output, Variable):
                output = Output(vr=output, pr=parent_ro, action=action)
            else:
                output = Output(value=output, action=action) # Directly hard-coded value. May be a DataObject attribute.

            do_insert = output.id not in put_ids
            outlet.add_put(output, action=action, do_insert=do_insert)
        
        if return_conn:
            action.commit = True
            action.execute()
        
        return outlets_dict
    
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