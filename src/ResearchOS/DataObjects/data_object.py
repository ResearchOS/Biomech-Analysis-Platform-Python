"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from ResearchOS import ResearchObject

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""    
    
    def load(self) -> None:
        """Load data values from the database."""
        ResearchObject.load(self)