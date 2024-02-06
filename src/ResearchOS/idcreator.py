from ResearchOS.db_handler import DBHandler
from config import Config
import random

import os

os.environ["ENV"] = "dev"
config = Config()

abstract_id_len = config.abstract_id_len
instance_id_len = config.instance_id_len

class IDCreator():
    """Creates all ID's for the ResearchOS database."""

    def __init__(self, db_handler: DBHandler) -> None:
        """Initialize the IDCreator."""
        self.db_handler = db_handler
    
    def create_ro_id(self, cls, abstract: str = None, instance: str = None, is_abstract: bool = False) -> str:
        """Create a ResearchObject ID."""
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
            cursor = DBHandler.cursor()
            sql = f'SELECT object_id FROM {table_name} WHERE object_id = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
            elif is_abstract:
                raise ValueError("Abstract ID already exists.")
        return id   


    def create_action_id(self) -> str:
        """Create an Action ID."""
        pass