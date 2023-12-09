from pipeline_objects.pipeline_object import PipelineObject

class Plot(PipelineObject):
    
        prefix = "PL"    
    
        def new(name: str):
            return Plot(name = name)