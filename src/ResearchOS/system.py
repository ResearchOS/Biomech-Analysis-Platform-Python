from .research_object import ResearchObject

class System(ResearchObject):
    """The internal representation of the entire ResearchOS system.
    Used to manage settings like the current user object ID, etc. that can't be encapsulated by any other object because they're system-wide."""
 
    def __init__(self) -> None:
        """Initialize the System object."""        
        if self.object_exists():
            self.load()            
        else:
            self.new()                

    def new(self) -> None:
        """Initialize a new System object."""
        # Set class-specific attributes      
        self.current_user_object_id = None
        super().new()

    def load(self) -> None:
        """Load the System object from the database."""
        # Load attributes in a class-specific way.
        super().load()