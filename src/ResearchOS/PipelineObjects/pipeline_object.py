from ResearchOS.research_object import ResearchObject
from ResearchOS.action import Action

all_default_attrs = {}

computer_specific_attr_names = []

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views"""
    
    def _add_source_object_id(self, source_object_id: str, action: Action = None) -> None:
        """Add a source object ID to the current object."""
        target_object_id = self.id
        is_active = 1
        self._update_pipeline_edge(source_object_id, target_object_id, is_active, action)

    def _remove_source_object_id(self, source_object_id: str, action: Action = None) -> None:
        """Remove a source object ID from the current object."""
        target_object_id = self.id
        is_active = 0
        self._update_pipeline_edge(source_object_id, target_object_id, is_active, action)

    def _add_target_object_id(self, target_object_id: str, action: Action = None) -> None:
        """Add a target object ID to the current object."""
        source_object_id = self.id
        is_active = 1
        self._update_pipeline_edge(source_object_id, target_object_id, is_active, action)

    def _remove_target_object_id(self, target_object_id: str, action: Action = None) -> None:
        """Remove a target object ID from the current object."""
        source_object_id = self.id
        is_active = 0
        self._update_pipeline_edge(source_object_id, target_object_id, is_active, action)

    def _update_pipeline_edge(self, source_object_id: str, target_object_id: str, is_active: bool, action: Action = None) -> None:
        """Update one pipeline edge."""
        if action is None:
            action = Action(name = "update_pipeline_edges")
        sqlquery = f"INSERT INTO pipelineobjects_graph (action_id, source_object_id, target_object_id, is_active) VALUES ('{action.id}', '{source_object_id}', '{target_object_id}', {is_active})"        
        action.add_sql_query(sqlquery)
        action.execute()