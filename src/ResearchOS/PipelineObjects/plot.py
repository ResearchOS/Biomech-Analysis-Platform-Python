import time

import networkx as nx

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action
from ResearchOS.plot_runner import PlotRunner
from ResearchOS.vr_handler import VRHandler
from ResearchOS.build_pl import make_all_edges
from ResearchOS.sql.sql_runner import sql_order_result

all_default_attrs = {}
# For MATLAB
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

# Main attributes
all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["inputs"] = {}
all_default_attrs["outputs"] = {}
all_default_attrs["subset"] = None

# For batching
all_default_attrs["batch"] = None

computer_specific_attr_names = ["mfolder"]

class Plot(PipelineObject):
    
    prefix = "PL"

    def __init__(self, is_matlab: bool = all_default_attrs["is_matlab"],
                 mfolder: str = all_default_attrs["mfolder"],
                 mfunc_name: str = all_default_attrs["mfunc_name"],
                 method: str = all_default_attrs["method"],
                 level: str = all_default_attrs["level"],
                 inputs: dict = all_default_attrs["inputs"],
                 subset: str = all_default_attrs["subset"],
                 batch: str = all_default_attrs["batch"],
                 **kwargs):
        if self._initialized:
            return
        self.is_matlab = is_matlab
        self.mfolder = mfolder
        self.mfunc_name = mfunc_name
        self.method = method
        self.level = level
        self.inputs = inputs
        self.subset = subset
        self.batch = batch
        super().__init__(**kwargs)
    
    def run(self, action: Action = None, force_redo: bool = False, return_conn: bool = True) -> None:
        """Execute the attached method.

        Args:
            force_redo (bool, optional): _description_. Defaults to False.
        """
        from ResearchOS.PipelineObjects._runnables import run
        run(self, __file__, force_redo=force_redo, action=action, return_conn=return_conn)