import networkx as nx

from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type
from ResearchOS.PipelineObjects.process import Process

# 1. When running a Process's "set_inputs" or "set_outputs" the user will expect to have the inputs & outputs connected to the proper places after each run.
# However, when "building" the pipeline, doing that for each Process would be inefficient.
# Therefore, I am including a separate "build_pl" function that will be called after all the Processes have been created.
# This function will connect all the inputs and outputs of the Processes to the proper places.

# "Multi" mode.
# While the Inlets & Outlets and Inputs & Outputs are created in SQL when the Processes' settings are created, the Edges are created in SQL when the pipeline is built.
# This is because the Edges need to see all of the available Inlets & Outlets before they can be created.

# "Single" (non-Multi) mode.
# The Inlets & Outlets and Inputs & Outputs are created in SQL when the Processes' settings are created.
# Edges are created in SQL when the Processes' settings are created, using currently available Processes in memory.

def build_pl():
    """Builds the pipeline."""
    import src.research_objects.processes as pr
    all_pr_objs = import_objects_of_type(Process)

    for pr_obj in all_pr_objs:
        pr_obj.set_inputs()
        pr_obj.set_outputs()
