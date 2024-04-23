import os

import networkx as nx

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action

all_default_attrs = {}
# For MATLAB
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

# Main attributes
all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["input_vrs"] = {}
all_default_attrs["vrs_source_pr"] = {}
all_default_attrs["subset_id"] = None

# For static lookup trial
all_default_attrs["lookup_vrs"] = {}

# For batching
all_default_attrs["batch"] = None

computer_specific_attr_names = ["mfolder"]

class Stats(PipelineObject):
    
    prefix = "ST"

    def __init__(self, is_matlab: bool = all_default_attrs["is_matlab"],
                 mfolder: str = all_default_attrs["mfolder"],
                 mfunc_name: str = all_default_attrs["mfunc_name"],
                 method: str = all_default_attrs["method"],
                 level: str = all_default_attrs["level"],
                 input_vrs: dict = all_default_attrs["input_vrs"],
                 vrs_source_pr: dict = all_default_attrs["vrs_source_pr"],
                 subset_id: str = all_default_attrs["subset_id"],
                 lookup_vrs: dict = all_default_attrs["lookup_vrs"],
                 batch: str = all_default_attrs["batch"],
                 **kwargs):
        if self._initialized:
            return
        self.is_matlab = is_matlab
        self.mfolder = mfolder
        self.mfunc_name = mfunc_name
        self.method = method
        self.level = level
        self.input_vrs = input_vrs
        self.vrs_source_pr = vrs_source_pr
        self.subset_id = subset_id
        self.lookup_vrs = lookup_vrs
        self.batch = batch
        super().__init__(**kwargs)
    
    def run(self, force_redo: bool = False, action: Action = None, return_conn: bool = True) -> None:
        """Execute the attached method.

        Args:
            force_redo (bool, optional): _description_. Defaults to False.
        """
        from ResearchOS.PipelineObjects._runnables import run
        run(self, __file__, force_redo=force_redo, action=action, return_conn=return_conn)