from src.ResearchOS.DataObjects.data_object import DataObject
from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from src.ResearchOS.action import Action

from abc import abstractmethod

class User(DataObject, PipelineObject):

    prefix: str = "US"

    @abstractmethod
    def new_current(id: str = None, name: str = None):
        """Create a new user and set it as the current one."""        
        user = User(id = id, name = name)
        User.set_current_user_object_id(id)        
        return user

    @abstractmethod
    def get_current_user_object_id() -> str:
        """Get the ID of the current user."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT current_user_object_id FROM current_user"
        result = cursor.execute(sqlquery).fetchone()
        if result is None or len(result) == 0:
            return None
        if len(result) > 1:
            raise AssertionError("There are multiple current users.")
        return result[0]

    @abstractmethod
    def set_current_user_object_id(user_object_id: str) -> None:
        """Set the ID of the current user."""        
        sqlquery = f"INSERT INTO current_user (current_user_object_id) VALUES ('{user_object_id}')"
        action = Action(name = "Set current user" + user_object_id)
        action.add_sql_query(sqlquery)
        action.execute()    
        
    def get_current_project_id(self) -> str:
        """Return the current project object ID for the current user."""                     
        current_user_object_id = self.get_current_user_object_id()
        current_user = User(id = current_user_object_id)
        return current_user.current_project_id
    
    def set_current_project_id(self, project_id: str) -> None:
        """Set the current project object ID for the current user."""        
        current_user_object_id = self.get_current_user_object_id()
        current_user = User(id = current_user_object_id)
        current_user.current_project_id = project_id
    
if __name__=="__main__":
    user = User()
    print(user)