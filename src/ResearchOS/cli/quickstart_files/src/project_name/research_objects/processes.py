import ResearchOS as ros

from research_objects import variables as vr
from research_objects import subsets as ss

# TODO: Change the class name to the names of your Data Objects
from research_objects.data_objects import MyDataObject

# TODO: Change the import to import your Python function.
from process.tmp import example_fcn

# Example of a Process that runs MATLAB code. Note that the order of input and output variables matters for positional arguments!
pr = ros.Process(id = "PR1", name = "My Process", level = MyDataObject, is_matlab = True)
pr.set_input_vrs(input_var_name_in_code = vr.hard_coded_vr)
pr.set_output_vrs(output_var_name_in_code = vr.vr1)
pr.mfolder = "path/to/matlab_code/folder"
pr.mfunc_name = "my_matlab_function"
pr.subset_id = ss.ss1.id

# Example of a Process that runs Python code. Note that the order of input and output variables matters for positional arguments!
pr = ros.Process(id = "PR1", name = "My Process", level = MyDataObject)
pr.set_input_vrs(input_var_name_in_code = vr.hard_coded_vr)
pr.set_output_vrs(output_var_name_in_code = vr.vr1)
pr.method = example_fcn
pr.subset_id = ss.ss1.id