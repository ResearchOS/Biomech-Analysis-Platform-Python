from ResearchOS.research_object import ResearchObject
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler

all_default_attrs = {}

complex_attrs_list = []

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views"""
    
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
        sqlquery = f"INSERT INTO vr_dataobjects (action_id, dataobject_id, vr_id, is_active) VALUES ('{action.id}', '{self.id}', '{vr_id}', 0)"
        conn = ResearchObjectHandler.pool.get_connection()
        conn.execute(sqlquery)
        ResearchObjectHandler.pool.return_connection(conn)
        del self.__dict__[name]