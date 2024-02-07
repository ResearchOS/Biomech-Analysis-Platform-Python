import random, os, uuid, sqlite3

from ResearchOS.config import Config

config = Config()

abstract_id_len = config.immutable["abstract_id_len"]
instance_id_len = config.immutable["instance_id_len"]

class IDCreator():
    """Creates all ID's for the ResearchOS database."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Initialize the IDCreator."""
        self.conn = conn
    
    def create_ro_id(self, cls, abstract: str = None, instance: str = None, is_abstract: bool = False) -> str:
        """Create a ResearchObject ID following [prefix]XXXXXX_XXX."""
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
            cursor = self.db.conn.cursor()
            sql = f'SELECT object_id FROM {table_name} WHERE object_id = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
            elif is_abstract:
                raise ValueError("Abstract ID already exists.")
        return id   


    def create_action_id(self) -> str:
        """Create an Action ID using Python's builtin uuid4."""
        is_unique = False        
        conn = self.conn
        cursor = conn.cursor()
        while not is_unique:
            uuid_out = str(uuid.uuid4()) # For testing dataset creation.
            sql = f'SELECT action_id FROM actions WHERE action_id = "{uuid_out}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
        return uuid_out
    
    def _is_action_id(uuid: str) -> bool:
        """Check if a string is a valid UUID."""
        import uuid as uuid_module
        try:
            uuid_module.UUID(str(uuid))
        except ValueError:
            return False
        return True   