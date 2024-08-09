from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME


class RunnableFactory():    

    @classmethod    
    def create(cls, runnable_type: str):
        if runnable_type == PROCESS_NAME:
            runnable = ProcessType
        elif runnable_type == PLOT_NAME:
            runnable = PlotType
        elif runnable_type == STATS_NAME:
            runnable = StatsType
        elif runnable_type == LOGSHEET_NAME:
            runnable = LogsheetType
        return runnable    
    
def validate_inputs(inputs: dict):
    pass

def validate_outputs(outputs: dict):
    pass

def validate_path(path: str):
    pass

def validate_name(name: str):
    pass

def validate_subset(subset: str):
    pass
    
class ProcessType():
    
    @classmethod
    def validate(cls, attrs):
        pass
        # validate_inputs(attrs['inputs'])
        # validate_outputs(attrs['outputs'])
        # validate_path(attrs['path'])
        # validate_name(attrs['name'])
        # validate_subset(attrs['subset'])

class PlotType():

    @classmethod
    def validate(cls, attrs):
        pass
        # validate_inputs(attrs['inputs'])
        # validate_path(attrs['path'])
        # validate_name(attrs['name'])
        # validate_subset(attrs['subset'])

class StatsType():
    
    @classmethod
    def validate(cls, attrs):
        pass
        # validate_inputs(attrs['inputs'])
        # validate_path(attrs['path'])
        # validate_name(attrs['name'])
        # validate_subset(attrs['subset'])

class LogsheetType():
    
    @classmethod
    def validate(cls, attrs):
        pass
        # validate_inputs(attrs['inputs'])
        # validate_outputs(attrs['outputs'])
        # validate_path(attrs['path'])
        # validate_name(attrs['name'])