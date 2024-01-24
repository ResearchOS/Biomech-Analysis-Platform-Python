from abc import abstractmethod
import json

from ResearchOS import DataObject
from ResearchOS.action import Action

default_abstract_attrs = {}
default_instance_attrs = {}
default_instance_attrs["dataset_path"] = None
default_instance_attrs["schema"] = None


class Dataset(DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"
    _current_source_type_prefix = "PJ"
    _source_type_prefix = "PJ"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Dataset)
    
    def __str__(self):
        return super().__str__(default_instance_attrs.keys(), self.__dict__)
    
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""  
        attrs = {}
        if self.is_instance_object():
            attrs = default_instance_attrs  
        else:
            attrs = default_abstract_attrs    
        super().__init__(default_attrs = attrs, **kwargs)
    
    #################### Start class-specific attributes ###################
    def validate_dataset_path(self, path: str) -> None:
        """Validate the dataset path."""
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")        
        
    def validate_schema(self, schema: list) -> None:
        """Validate the data schema follows the proper format."""
        from ResearchOS import User
        from ResearchOS import Variable
        # TODO: Check that every element is unique, no repeats.
        if not isinstance(schema, list):
            raise ValueError("Schema must be provided as a list!")
        if len(schema) == 0:
            return # They're resetting the schema.
        if len(schema) == 1:
            raise ValueError("At least two elements required for the schema! Dataset is always first + one more")
        for x in schema:
            if not isinstance(x, type):
                raise ValueError("Schema must be provided as a list of ResearchObject types!")
        if User in schema:
            raise ValueError("Do not include the User object in the schema! It is implicitly assumed to be the first element in the list")
        if Variable in schema:
            raise ValueError("Do not include the Variable object in the schema! It is implicitly assumed to be the last element in the list")
        if Dataset != schema[0]:
            raise ValueError("Dataset must be the first element in the list! Each type after that is in sequentially 'decreasing' order.")
        
    def to_json_schema(self, schema: list[type], action: Action) -> None:
        """Placeholder to make the load happy so it doesn't try to use the default load."""
        # 1. Generate a new schema ID.
        schema_id = Dataset._create_uuid() # Data schema ID is a UUID.
        # 2. Convert the list of types to a list of str.
        str_schema = []
        if schema is None:
            return str_schema         
        for sch in schema:
            str_schema.append(sch.prefix)
        # 3. Convert the list of str to a json string.
        json_schema = json.dumps(str_schema)
        sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_ordered, dataset_id, action_id) VALUES ('{schema_id}', '{json_schema}', '{self.id}', '{action.id}')"
        action.add_sql_query(sqlquery)
        # Store the schema ID as an attribute of the dataset.
        self._default_store_obj_attr("schema", schema, json_schema, action = action)
        return json_schema

    def from_json_schema(self, json_schema: str) -> list:
        """Convert the data schema from json to list of types."""
        str_schema = json.loads(json_schema)
        classes = self._get_subclasses(DataObject)        
        types_schema = [] # Should return a list of DataObject classes.
        for sch in str_schema:
            for cls in classes:
                if sch == cls.prefix:                    
                    types_schema.append(cls)
        return types_schema      

    @abstractmethod
    def _create_uuid() -> str:
        """Create the schema_id (as uuid.uuid4()) for the data schema."""
        import uuid
        is_unique = False
        cursor = Action.conn.cursor()
        while not is_unique:
            uuid_out = str(uuid.uuid4()) # For testing dataset creation.            
            sql = f'SELECT schema_id FROM data_address_schemas WHERE schema_id = "{uuid_out}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
        return uuid_out
    
    #################### Start Source objects ####################
    def get_users(self) -> list:
        """Return a list of user objects that belong to this project. Identical to Project.get_users()"""
        from ResearchOS import User
        us_ids = self._get_all_source_object_ids(cls = User)
        return self._gen_obj_or_none(us_ids, User)

    #################### Start Target objects ####################
    def get_projects(self) -> list:
        """Return a list of project objects that use this dataset."""
        from ResearchOS import Project
        pj_ids = self._get_all_target_object_ids(cls = Project)
        return self._gen_obj_or_none(pj_ids, Project)
    
    def add_project_id(self, project_id: str):
        """Add a project to the dataset."""
        from ResearchOS import Project
        self._add_target_object_id(project_id, cls = Project)

    def remove_project_id(self, project_id: str):
        """Remove a project from the dataset."""
        from ResearchOS import Project        
        self._remove_target_object_id(project_id, cls = Project)

    def get_subjects(self) -> list:
        """Return a list of subject objects that belong to this dataset."""
        from ResearchOS import Subject
        sj_ids = self._get_all_target_object_ids(cls = Subject)
        return self._gen_obj_or_none(sj_ids, Subject)
    
    def add_subject_id(self, subject_id: str):
        """Add a subject to the dataset."""
        from ResearchOS import Subject
        self._add_target_object_id(subject_id, cls = Subject)

    def remove_subject_id(self, subject_id: str):
        """Remove a subject from the dataset."""
        from ResearchOS import Subject        
        self._remove_target_object_id(subject_id, cls = Subject)

    #################### Start class-specific methods ####################
    def open_dataset_path(self) -> None:
        """Open the dataset's path in the Finder/File Explorer."""
        path = self.dataset_path

if __name__=="__main__":
    from DataObjects.subject import Subject
    from DataObjects.trial import Trial
    from DataObjects.phase import Phase
    from ResearchOS.database_init import DBInitializer
    db = DBInitializer()
    
    d1 = Dataset("DS1")
    d1_1 = Dataset("DS1")
    # s1 = Subject(uuid = "SB1", dataset_uuid = "DS1")
    # s2 = Subject(uuid = "SB2", dataset_uuid = "DS1")
    # d1.subjects = ["SB1", "SB2"]

    # BETTER - EITHER OPTION
    s4 = Subject(uuid = "SB4", dataset_uuid = "DS1", dataset = d1)

    t1 = Trial(uuid = "TR1", visit = v1)
    t2 = Trial(uuid = "TR2", visit = v1)
    trials = Subject.find(name = "s1").Trial.find(task = "SLG")

    sql.query.where(name = "x").where(task = "SLG")


    p1 = Phase(uuid = "PH1", trial = [t1, t2])
    p2 = Phase(uuid = "PH2", trial = t1)
    t3.add_phase(p1)

    s4 = d1.add_subject("SB4")
    