from data_objects.data_object import DataObject
from pipeline_objects.pipeline_object import PipelineObject
from research_object import ResearchObject
from action import Action

from abc import abstractmethod

class User(DataObject, PipelineObject):
    
    def new(id: str = None, name: str = None):
        """Create a new user."""
        if ResearchObject.is_id(id = id):            
            user = User(id = id, name = name)
        else:
            user = User(id = ResearchObject.create_id(cls = User), name = name)
        return user
    
    @abstractmethod
    def set_current_user(id: str):
        """Set the user as the current one."""
        sqlquery = f"UPDATE settings SET current_user_id = '{id}'"
        Action.conn.cursor().execute(sqlquery)
        Action.conn.commit()
    
    def new_current(id: str = None, name: str = None):
        """Create a new user and set it as the current one."""
        action = Action.open(name = "New Current User " + name)
        user = User.new(id = id, name = name)
        User.set_current_user(id)
        action.close()
        return user
    
    def load(id: str, action_id: str = None):
        """Load a user from the database."""
        return ResearchObject.load(id = id, cls = User, action_id = action_id)
    
    def set_current_project(self, id: str):
        """Set a project as the current one for the user."""
        self.current_project_id = id
    
if __name__=="__main__":
    user = User.new()
    print(user)