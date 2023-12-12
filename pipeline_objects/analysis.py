from pipeline_objects.pipeline_object import PipelineObject
from research_object import ResearchObject
from pipeline_objects.logsheet import Logsheet

from action import Action

class Analysis(PipelineObject):

    prefix = "AN"    

    def new(name: str):
        action = Action(name = "New Analysis " + name)
        an = Analysis(name = name)
        action.close()
        return an
    
    def new_current(name: str):
        action = Action.open(name = "New Current Analysis " + name)
        an = Analysis.new(name = name)
        lg = Logsheet.new(name = an.name + "_Default")
        an.set_current_logsheet(lg.id)        
        action.close()
        return an
    
    def set_current_logsheet(self, id: str):
        """Set a logsheet as the current one for the analysis."""
        self.current_logsheet_id = id
    
    def load(id: str, action_id: str = None):
        return ResearchObject.load(id = id, cls = Analysis, action_id = action_id)