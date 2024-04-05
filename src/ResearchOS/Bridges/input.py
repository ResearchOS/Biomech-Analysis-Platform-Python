from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port

class Input(Port):
    """Input port to connect between DiGraphs."""

    def __init__(self, vr: "Variable" = None, pr: "source_type" = None, lookup_vr: "Variable" = None, show: bool = False):
        """Initializes the Input object"""
        self.vr = vr
        self.pr = pr
        self.lookup_vr = lookup_vr
        if not vr and not pr:
            show = True
        self.show = show

        if not vr and pr is not None:
            raise ValueError("Input must have a variable if a source PR/LG is specified.")