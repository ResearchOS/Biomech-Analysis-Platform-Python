from src.ResearchOS.config import TestConfig
from src.ResearchOS.SQL.database_init import DBInitializer
from unittest import TestCase

db_file = TestConfig.db_file

class TestRODigraph(TestCase):

    db_file: str = db_file

    def setup_class(self):
        db = DBInitializer(db_file)

    def teardown_class(self):
        import os
        os.remove(self.db_file)

    def test_add_edge_with_current_analysis_id(self):
        """Setting an analysis as current should also add an edge to the digraph."""
        from src.ResearchOS.PipelineObjects.project import Project
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        from src.ResearchOS.Digraph.rodigraph import ResearchObjectDigraph
        pj = Project()
        an = Analysis()
        pj.current_analysis_id = an.id
        rod = ResearchObjectDigraph()
        self.assertTrue(rod.has_edge(pj.id, an.id))

    def test_set_current_analysis_id(self):
        from src.ResearchOS.PipelineObjects.project import Project
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        pj = Project()
        an = pj.get_analyses()
        self.assertTrue(an is None)
        an = Analysis()
        pj.current_analysis_id = an.id
        curr_an = pj.get_analyses()[0]
        self.assertTrue(an.id == curr_an.id)

if __name__=="__main__":
    test = TestRODigraph()
    test.setup_class()
    test.test_set_current_analysis_id()
    test.teardown_class()