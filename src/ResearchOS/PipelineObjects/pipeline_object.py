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

computer_specific_attr_names = []

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views""" 
    
    def save_inputs(self, inputs: dict, action: Action) -> None:
        """Saving the input variables. is done in the input class."""
        pass    

    def save_outputs(self, outputs: dict, action: Action) -> None:
        """Saving the output variables. is done in the output class."""
        pass
        
    def load_inputs(self, action: Action) -> dict:
        """Load the input variables."""        
        return self.make_puts_dict_from_db(action, True)

    def load_outputs(self, action: Action) -> dict:
        """Load the output variables."""
        return self.make_puts_dict_from_db(action, False)
    
    def make_puts_dict_from_db(self, action: Action, is_input: bool) -> dict:
        """Load the input or output variables."""        
        # If is input, then source is null and target is not null. 
        # If is output, then source is not null and target is null.
        if is_input:
            col_name = "target_pr_id"
        else:
            col_name = "source_pr_id"
        sqlquery_raw = f"SELECT vr_name_in_code, source_pr_id, vr_id, hard_coded_value, order_num, is_lookup, show FROM pipeline WHERE is_active = 1 AND {col_name} = ? ORDER BY order_num ASC"
        sqlquery = sql_order_result(action, sqlquery_raw, [col_name], single = False, user = True, computer = False)
        params = (self.id,)
        result = action.conn.cursor().execute(sqlquery, params).fetchall()
        return self.make_puts_dict_from_db_result(result)
    
    def make_puts_dict_from_db_result(self, result: list) -> dict:
        """Given the results of a query on the pipeline table for one ResearchObject, return a dictionary of the inputs or outputs."""
        from ResearchOS.research_object import ResearchObject
        vr_names = [row[0] for row in result] if result else []
        inputs = empty_vr_dict(vr_names)
        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        for row in result:
            vr_name = row[0]
            source_pr_id = row[1]
            vr_id = row[2]
            hard_coded_value = json.loads(row[3]) if row[3] else None
            if isinstance(hard_coded_value, dict):
                prefix = [key for key in hard_coded_value.keys()][0]
                attr_name = [value for value in hard_coded_value.values()][0]
                cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix == prefix]
                if cls:
                    hard_coded_value = {cls[0]: attr_name}
            order_num = row[4]
            is_lookup = bool(row[5])
            show = row[6]
            inputs[vr_name]["show"] = show
            if not is_lookup:
                inputs[vr_name]["main"]["vr"] = vr_id if vr_id else hard_coded_value
                if source_pr_id:
                    while len(inputs[vr_name]["main"]["pr"]) <= order_num:
                        inputs[vr_name]["main"]["pr"].append(None)                
                    inputs[vr_name]["main"]["pr"][order_num] = source_pr_id                
            else:
                inputs[vr_name]["lookup"]["vr"] = vr_id
                if source_pr_id:
                    while len(inputs[vr_name]["lookup"]["pr"]) <= order_num:
                        inputs[vr_name]["lookup"]["pr"].append(None)
                    inputs[vr_name]["lookup"]["pr"][order_num] = source_pr_id if source_pr_id else []
        return inputs   
    
    def set_inputs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict.
        Edges are created here."""
        action = Action(name="Set Inputs")
        standardized_kwargs = self.make_puts_dict_from_inputs(kwargs, is_input=True, action=action)
        self.__dict__["inputs"] = standardized_kwargs
        write_puts_dict_to_db(self, puts=self.inputs, is_input=True, action=action)
        action.commit = True
        action.execute()

    def set_outputs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict.
        Edges are NOT created here."""
        action = Action(name="Set Outputs")
        standardized_kwargs = self.make_puts_dict_from_inputs(kwargs, is_input=False, action=action)
        self.__dict__["outputs"] = standardized_kwargs
        write_puts_dict_to_db(self, puts=self.outputs, is_input=False, action=action)
        action.commit = True
        action.execute()

    def make_puts_dict_from_inputs(self, all_puts: Union[Input, dict], action: Action = None, is_input: bool = True) -> dict:
        """Create the dictionary that is the equivalent of someone passing in a dictionary directly."""
        from ResearchOS.Digraph.pipeline_digraph import PipelineDiGraph
        G = PipelineDiGraph(action=action)
        final_dict = empty_vr_dict(all_puts.keys())
        for vr_name, put in all_puts.items():
            if isinstance(put, Variable):
                if put.hard_coded_value is None:
                    final_dict[vr_name]["main"]["vr"] = put.id
                    # Get the source_pr unless this is an import file VR.
                    if not (hasattr(self, "import_file_vr_name") and vr_name==self.import_file_vr_name):
                        if is_input:
                            pr = Input.set_source_pr(self, put, vr_name, G)
                        else:
                            pr = self
                        final_dict[vr_name]["main"]["pr"].append(pr.id if pr else None)
                else:
                    final_dict[vr_name]["main"]["vr"] = put.hard_coded_value
            elif isinstance(put, Input):
                final_dict[vr_name] = put.__dict__
                if put.main["vr"] and not put.main["pr"]:
                    pr = Input.set_source_pr(self, put.main["vr"], vr_name, G)
                    final_dict[vr_name]["main"]["pr"] = [pr.id]
                if put.lookup["vr"] and not put.lookup["pr"]:
                    lookup_pr = Input.set_source_pr(self, put.lookup["vr"], vr_name, G)
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
    sub_dict = {"show": False, "main": {"vr": None, "pr": []}, "lookup": {"vr": None, "pr": []}}
    final_dict = {key: copy.deepcopy(sub_dict) for key in keys}
    return final_dict