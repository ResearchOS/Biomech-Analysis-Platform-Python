from networkx import MultiDiGraph 

class ROGraph(MultiDiGraph):
    """This class represents a mutli-digraph of the relations of the pipeline objects to one another."""
    
    def __init__(self):
        """Load all of the pipeline objects in their current state."""
        # 1. Get a list of all pipeline objects that are not currently deleted.

        # 2. Load each pipeline object, adding their ID's (and attributes) to the digraph.

        # 3. Look in the research_objects_attributes table to see which objects are connected to each other. 
        # Construct an adjacency matrix or edge list.

