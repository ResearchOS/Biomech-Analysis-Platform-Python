from abc import abstractmethod

from src.ResearchOS import ResearchObject

from src.ResearchOS.DataObjects import DataObject
from src.ResearchOS.SQL.database_init import DBInitializer
from typing import Union

class Dataset(DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix = "DS"

    @abstractmethod
    def new_current(name: str) -> "Dataset":
        """Create a new dataset and set it as the current dataset for the current project."""
        from src.ResearchOS.DataObjects.dataset import Dataset
        from src.ResearchOS.PipelineObjects.project import Project
        ds = Dataset(name = name)
        pj = Project.get_current_project_id()
        pj = Project(id = pj)
        pj.set_current_dataset_id(ds.id)
        return ds
    
    #################### Start class-specific attributes ###################

    def get_data_path(self) -> str:
        """Return the data path."""
        # TODO: Read this from the database.
        return self.data_path

    def set_data_path(self, path: str) -> None:
        """Set the data path."""
        self.data_path = path

    def get_data_schema(self) -> list:
        """Return the data schema."""
        return self.data_schema

    def set_data_schema(self, schema: list) -> None:
        """Set the data schema."""
        self.data_schema = schema
    
    #################### Start Source objects ####################

    def get_users(self) -> list:
        """Return a list of user objects that belong to this project. Identical to Project.get_users()"""
        from src.ResearchOS.user import User
        us_ids = self._get_all_source_object_ids(cls = User)
        return [User(id = us_id) for us_id in us_ids]

    #################### Start Target objects ####################

    def get_projects(self) -> list:
        """Return a list of project objects that use this dataset."""
        from src.ResearchOS.PipelineObjects.project import Project
        pj_ids = self._get_all_target_object_ids(cls = Project)
        return [Project(id = pj_id) for pj_id in pj_ids]
    
    def add_project_id(self, project_id: str):
        """Add a project to the dataset."""
        from src.ResearchOS.PipelineObjects.project import Project
        self._add_target_object_id(project_id, cls = Project)

    def remove_project_id(self, project_id: str):
        """Remove a project from the dataset."""
        from src.ResearchOS.PipelineObjects.project import Project        
        self._remove_target_object_id(project_id, cls = Project)

    def get_subjects(self) -> list:
        """Return a list of subject objects that belong to this dataset."""
        from src.ResearchOS.DataObjects.subject import Subject
        sj_ids = self._get_all_target_object_ids(cls = Subject)
        return [Subject(id = sj_id) for sj_id in sj_ids]
    
    def add_subject_id(self, subject_id: str):
        """Add a subject to the dataset."""
        from src.ResearchOS.DataObjects.subject import Subject
        self._add_target_object_id(subject_id, cls = Subject)

    def remove_subject_id(self, subject_id: str):
        """Remove a subject from the dataset."""
        from src.ResearchOS.DataObjects.subject import Subject        
        self._remove_target_object_id(subject_id, cls = Subject)

if __name__=="__main__":
    from DataObjects.subject import Subject
    from DataObjects.trial import Trial
    from DataObjects.phase import Phase
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
    