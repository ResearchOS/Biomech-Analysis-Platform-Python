from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port

class Output(Port):
    """Output port to connect between DiGraphs."""

    def __init__(self, vr: "Variable" = None, show: bool = False):
        """Initializes the Input object"""
        self.vr = vr
        if not vr:
            show = True
        self.show = show    