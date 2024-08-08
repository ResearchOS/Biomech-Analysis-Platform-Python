import random, uuid, sqlite3
import uuid
import random

from ResearchOS.config import Config
# from ResearchOS.sqlite_pool import SQLiteConnectionPool

config = Config("Immutable")

abstract_id_len = config.abstract_id_len
instance_id_len = config.instance_id_len

class IDCreator():
    """Creates all ID's for the ResearchOS database."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Initialize the IDCreator."""        
        self.conn = conn

    def get_prefix(self, id: str) -> str:
        """Get the prefix of the given ID."""
        if not self.is_ro_id(id):
            raise ValueError("The given ID is not a valid ResearchObject ID.")
        return id[:2]
    
    def create_ro_id(self, cls, abstract: str = None, instance: str = None, is_abstract: bool = False) -> str:
        """Create a ResearchObject ID following [prefix]XXXXXX_XXX."""
        conn = self.conn
        table_name = "research_objects"
        is_unique = False
        while not is_unique:
            if not abstract:
                abstract_new = str(hex(random.randrange(0, 16**abstract_id_len))[2:]).upper()
                abstract_new = "0" * (abstract_id_len-len(abstract_new)) + abstract_new
            else:
                abstract_new = abstract
            
            if not instance:
                instance_new = str(hex(random.randrange(0, 16**instance_id_len))[2:]).upper()
                instance_new = "0" * (instance_id_len-len(instance_new)) + instance_new
            else:
                instance_new = instance
            if is_abstract:
                instance_new = ""
 
            id = cls.prefix + abstract_new + "_" + instance_new
            cursor = conn.cursor()
            cursor = conn.cursor()
            sql = f'SELECT object_id FROM {table_name} WHERE object_id = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
            elif is_abstract:
                raise ValueError("Abstract ID already exists.")
        # self.pool.return_connection(conn)
        return id   


    def create_action_id(self, check: bool = True) -> str:
        """Create an Action ID using Python's builtin uuid4."""
        is_unique = False
        conn = self.conn
        cursor = conn.cursor()
        uuid_out = str(uuid.uuid4()) # For testing dataset creation.
        if not check:
            is_unique = True # If not checking, assume it's unique.
        while not is_unique:            
            sql = f'SELECT action_id FROM actions WHERE action_id = "{uuid_out}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
                break
            uuid_out = str(uuid.uuid4())
        # self.pool.return_connection(conn)
        return uuid_out
    
    def _is_action_id(uuid: str) -> bool:
        """Check if a string is a valid UUID."""
        import uuid as uuid_module
        try:
            uuid_module.UUID(str(uuid))
        except ValueError:
            return False
        return True   
    
    def is_ro_id(self, id: str) -> bool:
        """Check if the given ID matches the pattern of a valid research object ID."""    
        # TODO: Re-implement this when I decide on what the ResearchObject ID's look like.
        from ResearchOS.research_object import ResearchObject
        from ResearchOS.research_object_handler import ResearchObjectHandler
        instance_pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}_[a-fA-F0-9]{3}$"
        abstract_pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}$"
        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        if not any(id.startswith(cls.prefix) for cls in subclasses if hasattr(cls, "prefix")):
            return False
        return True

    def create_generic_id(self, table_name: str, id_name: str) -> str:
        """Create a generic ID for the given table."""
        conn = self.conn
        cursor = conn.cursor()
        is_unique = False
        while not is_unique:
            id = random.randint(1, 1000000)
            sql = f'SELECT {id_name} FROM {table_name} WHERE {id_name} = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
            # Check in the action add_sql_query.            
        return id