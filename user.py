from data_objects.data_object import DataObject
from pipeline_objects.pipeline_object import PipelineObject

class User(DataObject, PipelineObject):
    
    def new():
        """Create a new user."""
        user = User()
        return user