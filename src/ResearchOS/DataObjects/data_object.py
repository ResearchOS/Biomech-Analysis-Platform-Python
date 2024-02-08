"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from ResearchOS.research_object import ResearchObject

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""    
    
    def load(self) -> None:
        """Load data values from the database."""
        # 1. Identify which rows in the database are associated with this data object and have not been overwritten.

        # 2. Load the data values from the database.

        # 3. Set the data values in the data object's "vr" attribute.