import os
from typing import Any, TYPE_CHECKING

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.code_runner import CodeRunner

from ResearchOS.Bridges.input import Input
from ResearchOS.variable import Variable

from ResearchOS.var_converter import convert_var
from ResearchOS.DataObjects.data_object import DataObject

# Exit codes:
# 0: Success

class VRValue():

    def __init__(self, input: Input, code_runner: "CodeRunner", G: nx.MultiDiGraph) -> None:
        self.exit_code = 0
        self.value = self.get_value(input, code_runner, G)

    def get_value(self, input: Input, code_runner: "CodeRunner", G: nx.MultiDiGraph) -> Any:
        """Get the value of the input Variable."""
        vr = input.vr
        node_lineage = code_runner.node.get_node_lineage(code_runner.node, code_runner.dobj_ids, code_runner.paths)  
        # DataObject attribute.
        if isinstance(vr, dict) and len(vr) == 1 and isinstance([key for key in vr.keys()][0], DataObject):
            cls = [key for key in vr.keys()][0]
            node = [node for node in node_lineage if isinstance(node, cls)][0]
            attr = [value for value in vr.values()][0]
            return getattr(node, attr)
        
        # Specified directly as the hard-coded value, not using a Variable. Also is not a DataObject attribute.
        if not isinstance(vr, Variable):
            value = convert_var(vr, code_runner.matlab_numeric_types)
            return value
        
        # Specified as a Variable, and is hard-coded.
        if vr.hard_coded_value is not None:
            return convert_var(vr.hard_coded_value, code_runner.matlab_numeric_types)
        
        # Import file VR.              
        if hasattr(input.pr, "import_file_vr_name") and input.var_name_in_code == input.parent_ro.import_file_vr_name and input.pr.import_file_vr_name is not None:            
            data_path = code_runner.dataset.dataset_path
            # Isolate the parts of the ordered schema that are present in the file schema.
            file_node_lineage = [node for node in node_lineage if isinstance(node, tuple(dataset.file_schema))]
            for node in file_node_lineage[1::-1]:
                data_path = os.path.join(data_path, node.name)
            file_path = data_path + input.pr.import_file_ext
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"{input.pr.import_file_ext} file does not exist for {node.name} ({node.id}).")
            return file_path
        
        # Lookup VR.
        if input.lookup_vr is not None:
            lookup_input = Input(vr=input.lookup_vr, parent_ro=input.parent_ro, pr=input.pr)
            lookup_vrvalue = VRValue(lookup_input, code_runner, G)            
            pl_obj = lookup_vrvalue.value
            node_lineage = code_runner.get_node_lineage(lookup_input.vr, G)
        else:
            pl_obj = input.pr

        return code_runner.node.get(vr, code_runner.action, pl_obj, input.vr_name_in_code, node_lineage)

        


        
