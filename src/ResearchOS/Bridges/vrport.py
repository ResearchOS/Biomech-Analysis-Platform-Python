from typing import TYPE_CHECKING

from ResearchOS.PipelineObjects.process import Process
from ResearchOS.PipelineObjects.logsheet import Logsheet    
from variable import Variable
from port import Port

class VRPort(Port):
    """Responsible for managing the Variable (VR) ports between DiGraphs."""

    prefix: str = "VRPort"

    @staticmethod
    def from_str(str_in: str) -> "VRPort":
        """Converts a string to a VRPort object."""
        if not str_in.startswith("VRPort"):
            return None
        # Split the string by "," into a list.
        str_list = str_in.split("(")
        str_list = str_list[1].split(")")[0]

        _dict = {}        
        _dict["vr"] = Variable(id = str_list[0])
        if str_list[1].startswith("PR"):
            _dict["research_object"] = Process(id = str_list[1])
        else:
            _dict["research_object"] = Logsheet(id = str_list[1])
        _dict["is_input"] = bool(str_list[2])
        _dict["vr_name_in_code"] = str_list[3]

        return VRPort(**_dict)

    def to_str(self) -> str:
        """Returns the str name of the VRPort object."""
        json_name = "VRPort"
        if not self.vr:
            return json_name
    
        suffix = ",".join([self.vr.id, self.research_object.id, str(self.is_input), self.vr_name_in_code])
        json_name += "(" + suffix + ")"
        return json_name

    def __init__(self, vr: "Variable" = None, vr_specs: dict = None, **kwargs):
        """Initializes the VRPort object. The constructor will be executed before "set_input_vrs" and "set_output_vrs" methods.
        vr: Variable object. If this is specified, it means that the VR will be used within and outside of the graph.
        vr_specs: dict. Specifies criteria for a VR to be accepted."""
        _dict = {}
        _dict["vr"] = vr
        _dict["vr_specs"] = vr_specs
        # kwargs are supplied in the constructor when loading from string.
        for kwarg in kwargs:
            _dict[kwarg] = kwargs[kwarg]
        self.__dict__.update(_dict)

    def add_attrs(self, process_object: "Process", is_input: bool, vr_name_in_code: str):
        """Adds the attributes to the VRPort object.
        Must be run from within the to_json_input_vrs and to_json_output_vrs methods."""
        # 1. Ensure that the research_object is a Process object
        assert process_object.__class__.__name__ == "Process", "The research_object must be a Process object."
        _dict = {}
        _dict["research_object"] = process_object
        _dict["is_input"] = is_input
        _dict["vr_name_in_code"] = vr_name_in_code
        self.__dict__.update(_dict)  