from typing import TYPE_CHECKING, Union
import copy

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet   
    source_type = Union[Process, Logsheet]
    from ResearchOS.research_object import ResearchObject

from ResearchOS.variable import Variable
from ResearchOS.Bridges.input import Input
from ResearchOS.action import Action

class VRHandler():

    def empty_vr_dict(keys: list) -> dict:
        """Return a dictionary with the keys as None."""
        sub_dict = {"show": False, "main": {"vr": None, "pr": []}, "lookup": {"vr": None, "pr": []}}
        final_dict = {key: copy.deepcopy(sub_dict) for key in keys}
        return final_dict

    @staticmethod
    def standardize_lets_puts(parent_ro: "ResearchObject", all_puts: Union[Input, dict], action: Action = None, is_input: bool = True) -> dict:
        """Create the dictionary that is the equivalent of someone passing in a dictionary directly."""
        final_dict = VRHandler.empty_vr_dict(all_puts.keys())
        for vr_name, put in all_puts.items():
            if isinstance(put, Variable):
                if put.hard_coded_value is None:
                    final_dict[vr_name]["main"]["vr"] = put.id
                    # Get the source_pr unless this is an import file VR.
                    if not (hasattr(parent_ro, "import_file_vr_name") and vr_name==parent_ro.import_file_vr_name):
                        if is_input:
                            pr = Input.set_source_pr(parent_ro, put, vr_name)
                        else:
                            pr = parent_ro
                        final_dict[vr_name]["main"]["pr"].append(pr.id)
                else:
                    final_dict[vr_name]["main"]["vr"] = put.hard_coded_value
            elif isinstance(put, Input):
                final_dict[vr_name] = put.__dict__
                if put.main["vr"] and not put.main["pr"]:
                    pr = Input.set_source_pr(parent_ro, put.main["vr"], vr_name)
                    final_dict[vr_name]["main"]["pr"] = [pr.id]
                if put.lookup["vr"] and not put.lookup["pr"]:
                    lookup_pr = Input.set_source_pr(parent_ro, put.lookup["vr"], vr_name)
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