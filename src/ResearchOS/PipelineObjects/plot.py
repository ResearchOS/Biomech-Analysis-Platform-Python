from ResearchOS.PipelineObjects import PipelineObject

class Plot(PipelineObject):
    
        prefix = "PL"    
    
        def new(name: str):
            return Plot(name = name)