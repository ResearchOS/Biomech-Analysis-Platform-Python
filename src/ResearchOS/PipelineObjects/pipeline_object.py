from typing import Union
import json, copy

from ResearchOS.variable import Variable
from ResearchOS.research_object import ResearchObject
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.Digraph.pipeline_digraph import write_puts_dict_to_db
from ResearchOS.Bridges.input import Input

all_default_attrs = {}
all_default_attrs["up_to_date"] = False

computer_specific_attr_names = []

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views""" 

    def __init__(self, up_to_date: bool = all_default_attrs["up_to_date"], **kwargs) -> None:
        self.up_to_date = up_to_date
        super().__init__(**kwargs)
    
    def save_inputs(self, inputs: dict, action: Action) -> None:
        """Saving the input variables. is done in the input class."""
        pass    

    def save_outputs(self, outputs: dict, action: Action) -> None:
        """Saving the output variables. is done in the output class."""
        pass
        
    def load_inputs(self, action: Action) -> dict:
        """Load the input variables."""        
        return self.make_puts_dict_from_db(action, is_input=True)

    def load_outputs(self, action: Action) -> dict:
        """Load the output variables."""
        return self.make_puts_dict_from_db(action, is_input=False)
    
    def make_puts_dict_from_db(self, action: Action, is_input: bool) -> dict:
        """Load the input or output variables."""        
        # (row_id, action_id_num, ro_id, vr_name_in_code, vr_id, pr_ids, lookup_vr_id, lookup_pr_ids, hard_coded_value, is_input, show, is_active)
        sqlquery_raw = f"SELECT ro_id, vr_name_in_code, vr_id, pr_ids, lookup_vr_id, lookup_pr_ids, hard_coded_value, is_input, show FROM nodes WHERE ro_id = ? AND is_input = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["vr_name_in_code"], single = True, user = True, computer = False)
        params = (self.id, int(is_input))
        result = action.conn.cursor().execute(sqlquery, params).fetchall()
        # Get rid of outdated puts.
        del_list = []
        for row in result:
            if row[8]==0:
                del_list.append(row)
        for row in del_list:
            result.remove(row)
        return self.make_puts_dict_from_db_result(result)
    
    def make_puts_dict_from_db_result(self, result: list) -> dict:
        """Given the results of a query on the pipeline table for one ResearchObject, return a dictionary of the inputs or outputs.
        (row_id, action_id_num, ro_id, vr_name_in_code, vr_id, pr_ids, lookup_vr_id, lookup_pr_ids, hard_coded_value, is_input, show, is_active)
        If output, the dict contains only {vr_name: VR} filled in, the rest is left empty.
        If input, the dict contains {vr_name: {"main": {"vr": VR, "pr": [PR]}, "lookup": {"vr": VR, "pr": [PR]}, "show": bool}} filled in."""
        from ResearchOS.research_object import ResearchObject
        vr_names = [row[1] for row in result] if result else []
        puts = empty_vr_dict(vr_names)
        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        for row in result:
            # ro_id = row[0]
            vr_name = row[1]
            vr_id = row[2]
            pr_ids = json.loads(row[3])
            lookup_vr_id = row[4]
            lookup_pr_ids = json.loads(row[5])
            hard_coded_value = json.loads(row[6]) if row[6] else None
            # is_input = bool(row[7])
            show = row[8]                        
            if isinstance(hard_coded_value, dict):
                prefix = [key for key in hard_coded_value.keys()][0]
                attr_name = [value for value in hard_coded_value.values()][0]
                cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix == prefix]
                if cls:
                    hard_coded_value = {cls[0]: attr_name}

            puts[vr_name]["show"] = show
            puts[vr_name]["main"]["vr"] = vr_id if vr_id else hard_coded_value
            puts[vr_name]["main"]["pr"] = pr_ids  
            puts[vr_name]["lookup"]["vr"] = lookup_vr_id
            puts[vr_name]["lookup"]["pr"] = lookup_pr_ids
        return puts   
    
    def set_inputs(self, action: Action = None, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict.
        Edges are created here."""
        return_conn = False
        if not action:
            action = Action(name="Set Inputs")
            return_conn = True
        prev_puts = self.__dict__["inputs"]
        standardized_kwargs = self.make_puts_dict_from_inputs(kwargs, is_input=True, action=action, prev_puts=prev_puts)          
        self.__dict__["inputs"] = standardized_kwargs      
        write_puts_dict_to_db(self, puts=standardized_kwargs, is_input=True, prev_puts=prev_puts, action=action)        
        if return_conn:
            action.commit = True
            action.execute()

    def set_outputs(self, action: Action = None, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict.
        Edges are NOT created here."""
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        return_conn = False
        if not action:     
            return_conn = True       
            action = Action(name="Set Outputs")
        prev_puts = {}
        if not isinstance(self, Logsheet):
            prev_puts = self.__dict__["outputs"]  
        standardized_kwargs = self.make_puts_dict_from_inputs(kwargs, is_input=False, action=action, prev_puts=prev_puts)                  
        self.__dict__["outputs"] = standardized_kwargs
        write_puts_dict_to_db(self, puts=standardized_kwargs, is_input=False, action=action, prev_puts = prev_puts)
        if return_conn:
            action.commit = True
            action.execute()

    def make_puts_dict_from_inputs(self, all_puts: Union[Input, dict], action: Action = None, is_input: bool = True, prev_puts: dict = {}) -> dict:
        """Create the dictionary that is the equivalent of someone passing in a dictionary directly."""
        from ResearchOS.Digraph.pipeline_digraph import PipelineDiGraph
        G = PipelineDiGraph(action=action).G
        final_dict = empty_vr_dict(all_puts.keys())
        for vr_name, put in all_puts.items():
            if isinstance(put, Variable):
                if put.hard_coded_value is None:    
                    # Get the slice.
                    if isinstance(put._slice, tuple):
                        slice_list = put.slice_to_list(put._slice)
                    else:
                        slice_list = put._slice
                    final_dict[vr_name]["slice"] = slice_list
                    # put._slice = None # Reset the slice.
                    final_dict[vr_name]["main"]["vr"] = put.id                
                    # Get the source_pr unless this is an import file VR.
                    if hasattr(self, "import_file_vr_name") and vr_name==self.import_file_vr_name:
                        continue
                    pr = final_dict[vr_name]["main"]["pr"]
                    if is_input:
                        if vr_name in prev_puts and prev_puts[vr_name]["main"]["pr"] and self.up_to_date:
                            pr = prev_puts[vr_name]["main"]["pr"]
                        else:
                            pr = Input.set_source_pr(self, put, G)
                            pr = [pr.id] if pr else []
                    if pr:
                        final_dict[vr_name]["main"]["pr"] = pr                        
                else:
                    final_dict[vr_name]["main"]["vr"] = put.hard_coded_value
            elif isinstance(put, Input):
                final_dict[vr_name] = put.__dict__
                if put.main["vr"] and not put.main["pr"]:
                    pr = Input.set_source_pr(self, put.main["vr"], G)
                    if pr:
                        final_dict[vr_name]["main"]["pr"] = [pr.id]
                if put.lookup["vr"] and not put.lookup["pr"]:
                    lookup_vr = Variable(id=put.lookup["vr"], action=action)
                    lookup_pr = Input.set_source_pr(self, lookup_vr, G)
                    if lookup_pr:
                        final_dict[vr_name]["lookup"]["pr"] = [lookup_pr.id]
            else: # Hard-coded value.
                if isinstance(put, dict):
                    cls = [key for key in put.keys()][0]
                    attr_name = [value for value in put.values()][0]
                    if isinstance(cls, type) and hasattr(cls, "prefix") and len(cls.prefix) == 2:
                        final_dict[vr_name]["main"]["vr"] = {cls.prefix: attr_name}
                else:
                    final_dict[vr_name]["main"]["vr"] = put

        return final_dict

def empty_vr_dict(keys: list) -> dict:
    """Return a dictionary with the keys as None."""
    sub_dict = {"slice": None, "show": False, "main": {"vr": None, "pr": []}, "lookup": {"vr": None, "pr": []}}
    final_dict = {key: copy.deepcopy(sub_dict) for key in keys}
    return final_dict