from typing import TYPE_CHECKING, Union
import json

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

class Port():
    """Base class for the various port mechanisms to connect between DiGraphs."""

    def __hash__(self) -> int:
        return self.vr_name_in_code

    def add_attrs(self, parent_ro: "source_type", vr_name_in_code: str) -> None:
        """Add the attributes about the Process/Logsheet parent object to the Port object."""
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code

    def to_serializable(self) -> dict:
        """Returns a serializable dictionary of the Port object."""
        _dict = self.__dict__
        tmp_dict = {}
        tmp_dict["vr"] = _dict["vr"].id
        tmp_dict["show"] = _dict["show"]
        # For inputs
        if "pr" in _dict:
            tmp_dict["pr"] = _dict["pr"].id
            tmp_dict["lookup_vr"] = _dict["lookup_vr"].id

        return tmp_dict
    
    def to_dict(self) -> dict:
        """Returns a dictionary of the Port object."""
        _dict = self.__dict__
        tmp_dict = {}
        tmp_dict["vr"] = _dict["vr"]
        tmp_dict["show"] = _dict["show"]
        # For inputs
        if "pr" in _dict:
            tmp_dict["pr"] = _dict["pr"]
            tmp_dict["lookup_vr"] = _dict["lookup_vr"]

        return tmp_dict
    
    @staticmethod
    def from_serialized_dict(serialized_dict: dict) -> "Port":
        """Converts a serialized dictionary to a Port object."""
        from ResearchOS.Bridges.input import Input
        from ResearchOS.Bridges.output import Output
        from ResearchOS.variable import Variable
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet

        vr = None
        pr = None
        lookup_vr = None
        show = serialized_dict["show"]

        if serialized_dict["vr"] is not None:
            vr = Variable(id = serialized_dict["vr"])

        if "pr" in serialized_dict:
            if serialized_dict["pr"].startswith(Process.prefix):
                pr = Process(id = serialized_dict["pr"])
            elif serialized_dict["pr"].startswith(Logsheet.prefix):
                pr = Logsheet(id = serialized_dict["pr"])
            if serialized_dict["lookup_vr"] is not None:
                lookup_vr = Variable(id = serialized_dict["lookup_vr"])
            return Input(vr = vr, pr = pr, lookup_vr = lookup_vr, show = show)
        else:
            return Output(vr = vr, show = show)

    def to_str(self) -> str:
        """Returns the str representation of the Port object."""
        excl_attrs = ["parent_ro", "vr_name_in_code"]
        _dict = self.__dict__
        tmp_dict = {}
        for kwarg in _dict:
            if kwarg in excl_attrs:
                continue
            tmp_dict[kwarg] = _dict[kwarg]
        return json.dumps(tmp_dict)
    
    @staticmethod
    def from_str(str_in: str, is_input: bool) -> "Port":
        """Converts a string to a Port object."""
        from ResearchOS.Bridges.input import Input
        from ResearchOS.Bridges.output import Output
        tmp_dict = json.loads(str_in)

        if is_input:
            return Input(**tmp_dict)
        else:
            return Output(**tmp_dict)


        