from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port

class Output(Port):
    """Output port to connect between DiGraphs."""

    def __init__(self, id: int = None,
                 vr: "Variable" = None,
                 pr: "source_type" = None,
                 show: bool = False):
        """Initializes the Output object. "vr" and "pr" together make up the main source of the output."""
        if vr is None and pr is None:
            show = True

        # Don't add id here.
        self.vr = vr
        self.pr = pr
        self.show = show
        self.let = None        

        if id is not None:
            self.load_from_db(id)
