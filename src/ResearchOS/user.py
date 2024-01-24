from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action

from abc import abstractmethod

default_instance_attrs = {}
default_instance_attrs["current_user"] = None
default_instance_attrs["current_project_id"] = None

default_abstract_attrs = {}

class User(DataObject, PipelineObject):

    prefix: str = "US"
    # _current_source_type_prefixes = None # placeholder
    _source_type_prefixes = None # placeholder    

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

    def validate_current_project_id(self, id: str) -> bool:
        """Validate the current project ID. If it is not valid, the value is rejected."""        
        if not isinstance(id, str):
            raise ValueError("Specified value is not a string!")
        if not self.is_id(id):
            raise ValueError("Specified value is not an ID!")
        parsed_id = self.parse_id(id)
        if parsed_id[0] != "PJ":
            raise ValueError("Specified ID is not a Project!")
        if not self.object_exists(id):
            raise ValueError("Project does not exist!")    

    # @abstractmethod
    # def new_current(id: str = None, name: str = None):
    #     """Create a new user and set it as the current one."""        
    #     user = User(id = id, name = name)
    #     User.set_current_user_object_id(id)        
    #     return user

    @abstractmethod
    def get_current_user_object_id() -> str:
        """Get the ID of the current user."""
        from ResearchOS import DBInitializer
        cursor = Action.conn.cursor()
        sqlquery = "SELECT action_id, current_user_object_id FROM current_user"        
        result = cursor.execute(sqlquery).fetchall()        
        if result is None or len(result) == 0:
            DBInitializer(remove = False).init_current_user_id() # But then what about if the database loses integrity after creation?
            cursor = Action.conn.cursor()
            result = cursor.execute(sqlquery).fetchall()  
            # raise ValueError("There is no current user. This should never happen!")
        ordered_result = User._get_time_ordered_result(result, action_col_num = 0)
        return ordered_result[0][1]

    @abstractmethod
    def set_current_user_object_id(user_object_id: str) -> None:
        """Set the ID of the current user."""
        action = Action(name = "Set current user" + user_object_id)
        sqlquery = f"INSERT INTO current_user (action_id, current_user_object_id) VALUES ('{action.id}', '{user_object_id}')"        
        action.add_sql_query(sqlquery)
        action.execute()
    
if __name__=="__main__":
    user = User()
    print(user)