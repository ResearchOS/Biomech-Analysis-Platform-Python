"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from ResearchOS.research_object import ResearchObject
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.action import Action

all_default_attrs = {}

computer_specific_attr_names = []

class DataObject(ResearchObject):
    """The parent class for all data objects. Data objects represent some form of data storage, and approximately map to statistical factors."""    

    def __delattr__(self, name: str, action: Action = None) -> None:
        """Delete an attribute. If it's a builtin attribute, don't delete it.
        If it's a VR, make sure it's "deleted" from the database."""
        default_attrs = DefaultAttrs(self).default_attrs
        if name in default_attrs:
            raise AttributeError("Cannot delete a builtin attribute.")
        if name not in self.__dict__:
            raise AttributeError("No such attribute.")
        if action is None:
            action = Action(name = "delete_attribute")
        vr_id = self.__dict__[name].id        
        params = (action.id, self.id, vr_id)
        if action is None:
            action = Action(name = "delete_attribute")
        action.add_sql_query(self.id, "vr_to_dobj_insert_inactive", params)
        action.execute()
        del self.__dict__[name]

    def load_dataobject_vrs(self) -> None:
        """Load data values from the database."""
        # 1. Get all of the latest address_id & vr_id combinations (that have not been overwritten) for the current schema for the current database.
        # Get the schema_id.
        # TODO: Put the schema_id into the data_values table.
        # 1. Get all of the VRs for the current object.
        from ResearchOS.variable import Variable

        sqlquery = "SELECT vr_id FROM vr_dataobjects WHERE dataobject_id = ? AND is_active = 1"
        params = (self.id,)
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()
        vr_ids = cursor.execute(sqlquery, params).fetchall()
        vr_ids = [x[0] for x in vr_ids]
        ResearchObjectHandler.pool.return_connection(conn)
        for vr_id in vr_ids:
            vr = Variable(id = vr_id)
            self.__dict__[vr.name] = vr