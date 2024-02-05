from .research_object import ResearchObject

class System(ResearchObject):
    """The internal representation of the entire ResearchOS system.
    Used to manage settings like the current user object ID, etc. that can't be encapsulated by any other object because they're system-wide."""

    def __init__(self) -> None:
        """Initialize the System object."""
        if self.object_exists():
            self.load()
            return
        
        self.current_user_object_id = None

    def load(self) -> None:
        """Load the System object from the database."""
        # Does this need a custom implementation here?
        super().load()

    def get_current_user_object_id(self) -> None:
        """Get the current user object ID."""
        self.current_user_object_id
    
    def set_current_user_object_id(self, user_object_id: str) -> None:
        """Set the current user object ID."""
        self.current_user_object_id = user_object_id