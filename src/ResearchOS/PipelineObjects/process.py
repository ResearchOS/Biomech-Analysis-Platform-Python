from typing import Any, Callable

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action

all_default_attrs = {}
# For import
all_default_attrs["import_file_ext"] = None
all_default_attrs["import_file_vr_name"] = None

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

# For including other Data Object attributes from the node lineage in the input variables.
# For example, if a Process is run on a Trial, and one of the inputs needs to be the Subject's name.
# Then, "data_object_level_attr" would be "{ros.Subject: 'name'}"
# NOTE: This is always the last input variable(s), in the order of the input variables dict.
# all_default_attrs["data_object_level_attr"] = {}

# For batching
all_default_attrs["batch"] = None

computer_specific_attr_names = ["mfolder"]

do_run = False

class Process(PipelineObject):

    prefix = "PR"

    def __init__(self, is_matlab: bool = all_default_attrs["is_matlab"],
                 mfolder: str = all_default_attrs["mfolder"], 
                 mfunc_name: str = all_default_attrs["mfunc_name"], 
                 method: Callable = all_default_attrs["method"], 
                 level: type = all_default_attrs["level"], 
                 inputs: dict = all_default_attrs["inputs"], 
                 outputs: dict = all_default_attrs["outputs"], 
                 subset: str = all_default_attrs["subset"], 
                 import_file_ext: str = all_default_attrs["import_file_ext"], 
                 import_file_vr_name: str = all_default_attrs["import_file_vr_name"],
                 batch: list = all_default_attrs["batch"],
                 **kwargs) -> None:
        if self._initialized:
            return
        self.is_matlab = is_matlab
        self.mfolder = mfolder
        self.mfunc_name = mfunc_name
        self.method = method
        self.level = level
        self.inputs = inputs
        self.outputs = outputs
        self.subset = subset
        self.import_file_ext = import_file_ext
        self.import_file_vr_name = import_file_vr_name
        self.batch = batch
        super().__init__(**kwargs)                                                                        
        
    ## import_file_ext
        
    def validate_import_file_ext(self, file_ext: str, action: Action, default: Any) -> None:
        if file_ext == default:
            return
        if not self.import_file_vr_name and file_ext is None:
            return
        if self.import_file_vr_name and file_ext is None:
            raise ValueError("File extension must be specified if import_file_vr_name is specified.")
        if not isinstance(file_ext, str):
            raise ValueError("File extension must be a string.")
        if not file_ext.startswith("."):
            raise ValueError("File extension must start with a period.")
        
    ## import_file_vr_name
        
    def validate_import_file_vr_name(self, vr_name: str, action: Action, default: Any) -> None:
        if vr_name == default:
            return
        if not isinstance(vr_name, str):
            raise ValueError("Variable name must be a string.")
        if not str(vr_name).isidentifier():
            raise ValueError("Variable name must be a valid variable name.")
        

    def run(self, force_redo: bool = False, action: Action = None, return_conn: bool = True) -> None:
        """Execute the attached method.
        kwargs are the input VR's."""
        from ResearchOS.PipelineObjects._runnables import run
        run(self, __file__, force_redo=force_redo, action=action, return_conn=return_conn)
