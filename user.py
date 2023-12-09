from data_objects.data_object import DataObject
from pipeline_objects.pipeline_object import PipelineObject
from research_object import ResearchObject

class User(DataObject, PipelineObject):
    
    def new(id: str = None, name: str = None):
        """Create a new user."""
        if ResearchObject.is_id(id = id):            
            user = User(id = id, name = name)
        else:
            user = User(id = ResearchObject.create_id(cls = User), name = name)
        return user
    
if __name__=="__main__":
    user = User.new()
    print(user)