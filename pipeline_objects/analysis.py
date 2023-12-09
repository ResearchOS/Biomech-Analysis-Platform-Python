from pipeline_objects.pipeline_object import PipelineObject

class Analysis(PipelineObject):

    prefix = "AN"    

    def new(name: str):
        return Analysis(name = name)