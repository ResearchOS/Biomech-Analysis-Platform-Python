import weakref

from ResearchOS.db_connection_factory import DBConnectionFactory

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.

    @staticmethod
    def check_inputs(tmp_kwargs: dict) -> None:
        """Validate the inputs to the constructor."""
        # Convert the keys of the tmp_kwargs to lowercase.
        kwargs = {}
        for key in tmp_kwargs.keys():
            kwargs[str(key).lower()] = tmp_kwargs[key]
        if not kwargs or "id" not in kwargs.keys():
            raise ValueError("id is required as a kwarg")
        id = kwargs["id"]
        # Ensure that the ID is a string and is properly formatted.

        return kwargs
    
    @staticmethod
    def object_exists(id: str) -> bool:
        """Return true if the specified id exists in the database, false if not."""
        db = DBConnectionFactory.create_db_connection()
        cursor = db.conn.cursor()
        sqlquery = f"SELECT object_id FROM research_objects WHERE object_id = '{id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        return len(rows) > 0