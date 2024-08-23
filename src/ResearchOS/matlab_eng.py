import sys

import networkx as nx

def import_matlab(is_matlab: bool):
    # Import MATLAB
    if not is_matlab:
        return
    try:
        if "matlab" in sys.modules:
            matlab_double_types = (type(None), matlab.double,)
            matlab_numeric_types = (matlab.double, matlab.single, matlab.int8, matlab.uint8, matlab.int16, matlab.uint16, matlab.int32, matlab.uint32, matlab.int64, matlab.uint64)
        else:
            print("Importing MATLAB engine...")
            import matlab.engine
            matlab_double_types = (type(None), matlab.double,)
            matlab_numeric_types = (matlab.double, matlab.single, matlab.int8, matlab.uint8, matlab.int16, matlab.uint16, matlab.int32, matlab.uint32, matlab.int64, matlab.uint64)
            try:
                print("Attempting to connect to an existing shared MATLAB session.")                
                matlab_eng = matlab.engine.connect_matlab(name = "ResearchOS")
                print("Successfully connected to the shared 'ResearchOS' MATLAB session.")
            except:
                print("Failed to connect. Starting a new MATLAB session.")
                print("To share an existing session run <matlab.engine.shareEngine('ResearchOS')> in MATLAB's Command Window and leave MATLAB open.")
                matlab_eng = matlab.engine.start_matlab()
    except:
        raise ValueError("Failed to import MATLAB engine.")
    
    matlab_output = {
        "matlab_eng": matlab_eng,
        "matlab_double_types": matlab_double_types,
        "matlab_numeric_types": matlab_numeric_types
    }
    return matlab_output

def check_if_matlab(dag: nx.MultiDiGraph, nodes_to_run: list):
    """Check if any of the nodes to run require MATLAB."""
    return any([dag.nodes[node]['node'].language == "matlab" for node in nodes_to_run])