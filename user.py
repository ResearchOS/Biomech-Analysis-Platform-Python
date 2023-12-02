from data_objects.data_object import DataObject
from pipeline_objects.pipeline_object import PipelineObject

class User(DataObject, PipelineObject):
    
    def new(name: str = None):
        """Create a new user."""
        user = User()
        return user
    
if __name__=="__main__":
    user = User.new()
    print(user)