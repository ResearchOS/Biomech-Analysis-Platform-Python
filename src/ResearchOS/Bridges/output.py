from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.action import Action
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port
import ResearchOS.Bridges.input_types as it

class Output(Port):
    """Output port to connect between DiGraphs."""

    is_input: bool = False

    def __init__(self, id: int = None,
                 vr: "Variable" = None,
                 pr: "source_type" = None,
                 show: bool = False,
                 action: "Action" = None,
                 let: "InletOrOutlet" = None):
        """Initializes the Output object. "vr" and "pr" together make up the main source of the output."""
        if vr is None and pr is None:
            output = it.DynamicMain(show = True)
        else:
            output = it.DynamicMain(it.Dynamic(vr=vr, pr=pr), show=show)
        
        self.put_value = output
        self.action = action
        self.let = let

        super().__init__()
