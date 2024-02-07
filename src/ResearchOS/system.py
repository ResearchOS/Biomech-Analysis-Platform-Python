from ResearchOS.db_connection import DBConnectionFactory

class System():
    """The internal representation of the entire ResearchOS system.
    Used to manage settings like the current user object ID, etc. that can't be encapsulated by any other object because they're system-wide."""

    db = DBConnectionFactory.create_db_connection()
 
    def __init__(self, **kwargs) -> None:
        """Initialize the System object."""
        _current_user_object_id = self.current_user_object_id()                       

    @property
    def current_user_object_id(self) -> str:
        """Get the current user object ID from the database."""
        return self._current_user_object_id

    @current_user_object_id.setter
    def current_user_object_id(self, user_object_id: str) -> None:
        """Set the current user object ID in the database."""
        # 1. Update the setting in the database for the current user object ID.

        self._current_user_object_id = user_object_id
    