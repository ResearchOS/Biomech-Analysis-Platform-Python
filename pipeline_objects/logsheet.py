from pipeline_objects.pipeline_object import PipelineObject
from action import Action

class Logsheet(PipelineObject):
    
    def new(name: str) -> "Logsheet":
        action = Action(name = "New Logsheet" + name)
        pj = Logsheet(name = name)
        action.close()
        return pj