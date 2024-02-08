from typing import Any
import json

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["schema"]  = []

complex_attrs_list = ["schema"]

class Dataset(ros.DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"

    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name == "vr":
            ros.DataObject.__setattr__(self, name, value, action, validate)
        else:
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        self.load_schema() # Load the dataset schema.
        ros.DataObject.load(self) # Load the attributes specific to it being a DataObject.

    def save_schema(self, schema: list, action: Action) -> None:
        """Save the schema to the database."""
        # 2. Convert the list of types to a list of str.
        str_schema = []
        for edge_list in schema:
                str_schema.append([edge_list[0].prefix, edge_list[1].prefix])

        # 3. Convert the list of str to a json string.
        json_schema = json.dumps(str_schema)

        # 5. Save the schema to the database.
        conn = DBConnectionFactory.create_db_connection().conn
        sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_edge_list, dataset_id, action_id) VALUES ('{IDCreator(conn).create_action_id()}', '{json_schema}', '{self.id}', '{action.id}')"
        action.add_sql_query(sqlquery)
    
    #################### Start class-specific attributes ###################    

    def load_schema(self) -> None:
        """Load the schema from the database and convert it via json."""
        prefix_schema = None # Initialize the schema as None.
        # 1. Get the dataset ID
        id = self.id
        # 2. Get the most recent action ID for the dataset in the data_address_schemas table.
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE dataset_id = '{id}'"
        conn = DBConnectionFactory.create_db_connection().conn
        result = conn.execute(sqlquery).fetchall()
        # result = [_[0] for _ in result]
        ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num = 0)
        if len(ordered_result) > 0:
            most_recent_action_id = ordered_result[0][0]
            # 3. Get the schema from the levels_ordered column in the data_address_schemas table.
            sqlquery = f"SELECT levels_edge_list FROM data_address_schemas WHERE action_id = '{most_recent_action_id}'"
            result = conn.execute(sqlquery).fetchall()
            prefix_schema = json.loads(result[0][0])

        # 5. If the schema is not None, convert the string to a list of types.
        schema = []
        for edge_list in prefix_schema:
            schema.append([
                ResearchObjectHandler._prefix_to_class(self, edge_list[0]), ResearchObjectHandler._prefix_to_class(self, edge_list[1])
                ])

        # 6. Store the schema as an attribute of the dataset.
        self.__dict__["schema"] = schema

    


    def validate_dataset_path(self, path: str) -> None:
        """Validate the dataset path."""
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")        
        
    def validate_schema(self, schema: list) -> None:
        """Validate that the data schema follows the proper format.
        Must be an edge list [source, target], which is a list of lists (of length 2)."""
        # TODO: Check that every element is unique, no repeats.
        if not isinstance(schema, list):
            raise ValueError("Schema must be provided as a list!")
        if len(schema) == 0:
            return # They're resetting the schema.
        for idx, _ in enumerate(schema):
            if not isinstance(_, list):
                raise ValueError("Schema must be provided as a list of lists!")
            if len(_) != 2:
                raise ValueError("Schema must be provided as a list of lists of length 2!")
            if not isinstance(_[0], type) or not isinstance(_[1], type):
                raise ValueError("Schema must be provided as a list of lists of ResearchObject types!")
            if isinstance(_[0], ros.Variable) or isinstance(_[1], ros.Variable):
                raise ValueError("Do not include the Variable object in the schema! It is implicitly assumed to be the last element in the list")                
            if idx == 0 and _[0] != Dataset:
                raise ValueError("Dataset must be the first element in the first list as the origin/source node of the schema.")
        
    # def to_json_schema(self, schema: list[type], action: Action) -> None:
    #     """Placeholder to make the load happy so it doesn't try to use the default load."""
    #     # 1. Generate a new schema ID.
    #     schema_id = Dataset._create_uuid() # Data schema ID is a UUID.
    #     # 2. Convert the list of types to a list of str.
    #     str_schema = []
    #     if schema is None:
    #         return str_schema         
    #     for sch in schema:
    #         str_schema.append(sch.prefix)
    #     # 3. Convert the list of str to a json string.
    #     json_schema = json.dumps(str_schema)
    #     sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_ordered, dataset_id, action_id) VALUES ('{schema_id}', '{json_schema}', '{self.id}', '{action.id}')"
    #     action.add_sql_query(sqlquery)
    #     # Store the schema ID as an attribute of the dataset.
    #     self._default_store_obj_attr("schema", schema, json_schema, action = action)
    #     return json_schema

    # def from_json_schema(self, json_schema: str) -> list:
    #     """Convert the data schema from json to list of types."""
    #     str_schema = json.loads(json_schema)
    #     classes = self._get_subclasses(ros.DataObject)        
    #     types_schema = [] # Should return a list of DataObject classes.
    #     for sch in str_schema:
    #         for cls in classes:
    #             if sch == cls.prefix:                    
    #                 types_schema.append(cls)
    #     return types_schema

    # @abstractmethod
    # def _create_uuid() -> str:
    #     """Create the schema_id (as uuid.uuid4()) for the data schema."""
    #     import uuid
    #     is_unique = False
    #     cursor = Action.conn.cursor()
    #     while not is_unique:
    #         uuid_out = str(uuid.uuid4()) # For testing dataset creation.            
    #         sql = f'SELECT schema_id FROM data_address_schemas WHERE schema_id = "{uuid_out}"'
    #         cursor.execute(sql)
    #         rows = cursor.fetchall()
    #         if len(rows) == 0:
    #             is_unique = True
    #     return uuid_out
    
    # @abstractmethod
    # def get_current() -> "Dataset":
    #     """Return the current dataset for the current project for the current user."""
    #     from ResearchOS import User, Project
    #     us = User(id = User.get_current_user_object_id())
    #     pj = Project(id = us.current_project_id)
    #     ds = Dataset(id = pj.current_dataset_id)
    #     return ds
    
    # @abstractmethod
    # def create_data_objects(self, hierarchy: dict, action: Action = None) -> None:
    #     """Create the data objects that belong to this dataset."""
    #     self.validate_schema(self.schema)
    #     # Validate the keys of the hierarchy.
    #     ordered_hierarchy = []
    #     for sch in self.schema:
    #         for level in hierarchy.keys():
    #             if level is sch:
    #                 if not isinstance(hierarchy[level], int):
    #                     raise ValueError("Hierarchy must be provided as a dict of {level: int_objects}!")
    #                 ordered_hierarchy.append(level)
    #     # Validate the order of the hierarchy.
    #     indices = []
    #     for ord in ordered_hierarchy:
    #         idx = self.schema.indexof(ord)
    #         if any(idx < indices):
    #             raise ValueError("Hierarchy is out of order!")
                    
    
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
    from ResearchOS.db_initializer import DBInitializer
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
    