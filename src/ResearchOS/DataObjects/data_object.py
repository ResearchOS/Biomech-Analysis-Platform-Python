"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from ResearchOS.research_object import ResearchObject
from ResearchOS.research_object_handler import ResearchObjectHandler

all_default_attrs = {}

complex_attrs_list = []

# Root folder where all data is stored.
# root_data_path = "data"

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""    

    def load_data_values(self) -> None:
        """Load data values from the database."""
        # 1. Get all of the latest address_id & vr_id combinations (that have not been overwritten) for the current schema for the current database.
        # Get the schema_id.
        # TODO: Put the schema_id into the data_values table.
        # 1. Get all of the VRs for the current object.
        from ResearchOS.variable import Variable
        sqlquery = f"SELECT vr_id FROM vr_dataobjects WHERE dataobject_id = '{self.id}'"
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()
        vr_ids = cursor.execute(sqlquery).fetchall()
        vr_ids = [x[0] for x in vr_ids]
        ResearchObjectHandler.pool.return_connection(conn)
        for vr_id in vr_ids:
            vr = Variable(id = vr_id)
            self.__dict__[vr.name] = vr