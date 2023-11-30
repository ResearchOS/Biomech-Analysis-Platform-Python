from typing import Any
from action import Action

abstract_id_len = 6
instance_id_len = 3


class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""
    def __init__(self, name: str) -> None:
        self.id = self.create_id()
        self.name = name

    def __setattr__(self, __name: str, __value: Any) -> None:
        """Set the attributes of a research object in memory and in the SQL database."""
        
        self.__dict__[__name] = __value
        table_name = "ResearchObjects"
        cursor = Action.conn.cursor()
        # Create the object in the database, in the table that contains only the complete list of object ID's.        
        if __name == "id":                        
            sqlquery = f"INSERT INTO {table_name} (id) VALUES ('{self.id}')"            
            # cursor.execute(sqlquery)
            return
        
        # Modify the object's attributes in the database.
        attr_id = self.get_attr_id(__name)
        # sqlquery = f"INSERT INTO {table_name} (action_id, object_id, attr_id, attr_value) VALUES ('{Action.get_open().uuid}', '{self.id}', '{attr_id}', '{__value}')"        
        # cursor.execute(sqlquery)

    def get_attr_id(self, attr_name: str) -> int:
        """Get the ID of an attribute."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT id FROM Attributes WHERE name = '{attr_name}'"
        # cursor.execute(sqlquery)
        # rows = cursor.fetchall()
        # if len(rows) == 0:
        #     sqlquery = f"INSERT INTO Attributes (name) VALUES ('{attr_name}')"
        #     cursor.execute(sqlquery)
        #     sqlquery = f"SELECT id FROM Attributes WHERE name = '{attr_name}'"
        #     cursor.execute(sqlquery)
        #     rows = cursor.fetchall()
        # assert len(rows) == 1
        # return rows[0][0]

    def prefix_to_table_name(self) -> str:
        """Convert a prefix to a table name."""
        prefix = self.prefix
        if prefix in ["PJ", "AN", "LG", "PG", "PR", "ST", "VW"]:
            return "PipelineObjects"
        elif prefix in ["DS", "SJ", "TR", "PH"]:
            return "DataObjects"
        elif prefix in ["VR"]:
            raise NotImplementedError("Which table do Variables go in?")

    def create_id(self, abstract: str = None, instance: str = None) -> str:
        """Create a unique ID for the research object."""
        import random
        table_name = self.prefix_to_table_name()
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

            id = self.prefix + abstract_new + "_" + instance_new
            cursor = Action.conn.cursor()
            sql = f'SELECT id FROM {table_name} WHERE id = "{id}"'
            # cursor.execute(sql)
            # rows = cursor.fetchall()
            # if len(rows) == 0:
            is_unique = True
        return id
    
if __name__=="__main__":
    from pipeline_objects.project import Project
    pj = Project.new_current(name = "Test")
    print(pj)