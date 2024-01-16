from abc import abstractmethod

from ResearchOS import DataObject

default_attrs = {}
default_attrs["dataset_path"] = None
default_attrs["schema"] = None


class Dataset(DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Dataset)

    @abstractmethod
    def new_current(name: str) -> "Dataset":
        """Create a new dataset and set it as the current dataset for the current project."""        
        from ResearchOS import Project
        ds = Dataset(name = name)
        pj = Project.get_current_project_id()
        pj = Project(id = pj)
        pj.set_current_dataset_id(ds.id)
        return ds, pj
    
    def __str__(self):
        return super().__str__(default_attrs.keys(), self.__dict__)
    
    #################### Start class-specific attributes ###################
    def validate_dataset_path(self, path: str) -> None:
        """Validate the dataset path."""
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")        
        
    def validate_data_schema(self, schema: list) -> None:
        """Validate the data schema follows the proper format."""
        from ResearchOS import User
        from ResearchOS import Variable
        # TODO: Check that every element is unique, no repeats.
        if not isinstance(schema, list):
            raise ValueError("Schema must be provided as a list!")
        if len(schema) <= 1:
            raise ValueError("At least two elements required for the schema! Dataset + one more")
        for x in schema:
            if not isinstance(x, type):
                raise ValueError("Schema must be provided as a list of types!")
        if User in schema:
            raise ValueError("Do not include the User object in the schema! It is assumed to be the first element in the list")
        if Variable in schema:
            raise ValueError("Do not include the Variable object in the schema! It is assumed to be the last element in the list")
        if Dataset != schema[0]:
            raise ValueError("Dataset must be the first element in the list! Each type after that is in sequentially 'decreasing' order.")

    def json_translate_data_schema(self, schema) -> list:
        """Translate the data schema from to the proper value because the default json translation fails with this data structure."""
        pass

    def store_data_schema(self, schema, action):
        """Method to custom store the data schema."""
        pass
    
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
    