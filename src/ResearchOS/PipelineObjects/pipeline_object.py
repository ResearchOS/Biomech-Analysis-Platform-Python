from ResearchOS.research_object import ResearchObject
# from typing import Any

all_default_attrs = {}

complex_attrs_list = []

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views"""

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        pass