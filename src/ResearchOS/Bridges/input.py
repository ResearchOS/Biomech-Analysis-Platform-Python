from typing import TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port
from ResearchOS.Bridges.output import Output

class Input(Port):
    """Input port to connect between DiGraphs."""

    table_name: str = "inputs"
    id_col: str = "input_id"

    def __init__(self, id: int = None,
                 vr: "Variable" = None, 
                 pr: "source_type" = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,
                 value: Any = None,
                 show: bool = False,
                 src: "Output" = None):
        """Initializes the Input object. "vr" and "pr" together make up the main source of the input. "lookup_vr" and "lookup_pr" together make up the lookup source of the input.
        "value" is the hard-coded value. If specified, supercedes the main source."""                    

        if src is not None:
            vr = src.vr
            pr = src.pr                        
        
        if pr is None and vr is None:
            show = True

        # Don't add id here.
        self.vr = vr
        self.pr = pr
        self.lookup_vr = lookup_vr
        self.lookup_pr = lookup_pr
        self.show = show
        self.value = value
        self.let = None

        if id is not None:
            self.load_from_db(id) 