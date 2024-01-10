from src.ResearchOS.DataObjects.data_object import DataObject
from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from src.ResearchOS.action import Action

from abc import abstractmethod

default_instance_attrs = {}
default_instance_attrs["current_user_id"] = None

default_abstract_attrs = {}

class User(DataObject, PipelineObject):

    prefix: str = "US"

    def get_default_attrs(self):
        """Return a dictionary of default instance or abstract attributes, as appropriate for this object."""
        if self.is_instance_object():
            return default_instance_attrs
        return default_abstract_attrs
    
    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(User)
    
    def __str__(self):
        return super().__str__(self.get_default_attrs().keys(), self.__dict__)
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(default_attrs = self.get_default_attrs(), **kwargs)

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
        sqlquery = "SELECT action_id, current_user_object_id FROM current_user"        
        result = cursor.execute(sqlquery).fetchall()        
        if result is not None and len(result) > 1:
            raise AssertionError("There are multiple current users.")
        if result is None or len(result) == 0:
            raise ValueError("There is no current user. This should never happen!")
        ordered_result = User._get_time_ordered_result(result, action_col_num = 0)
        return ordered_result[0][1]

    @abstractmethod
    def set_current_user_object_id(user_object_id: str) -> None:
        """Set the ID of the current user."""
        action = Action(name = "Set current user" + user_object_id)
        sqlquery = f"INSERT INTO current_user (action_id, current_user_object_id) VALUES ('{action.id}', '{user_object_id}')"        
        action.add_sql_query(sqlquery)
        action.execute()
        
    # def get_current_project_id(self) -> str:
    #     """Return the current project object ID for the current user."""                     
    #     current_user_object_id = self.get_current_user_object_id()
    #     current_user = User(id = current_user_object_id)
    #     return current_user.current_project_id
    
    # def set_current_project_id(self, project_id: str) -> None:
    #     """Set the current project object ID for the current user."""                
    #     current_user = User(id = self.id)
    #     current_user.current_project_id = project_id
    
if __name__=="__main__":
    user = User()
    print(user)