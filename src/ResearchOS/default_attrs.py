import importlib

class DefaultAttrs():
    """A class to store the default attributes of a class across all three levels."""
    def __init__(self, cls: type):
        """Initialize the default attributes."""
        from ResearchOS.research_object import all_default_attrs as ro_default_attrs
        from ResearchOS.research_object import complex_attrs_list as ro_complex_attrs
        from ResearchOS.PipelineObjects.pipeline_object import all_default_attrs as p_default_attrs
        from ResearchOS.PipelineObjects.pipeline_object import complex_attrs_list as p_complex_attrs
        from ResearchOS.DataObjects.data_object import all_default_attrs as d_default_attrs
        from ResearchOS.DataObjects.data_object import complex_attrs_list as d_complex_attrs

        from ResearchOS.DataObjects.data_object import DataObject
        from ResearchOS.PipelineObjects.pipeline_object import PipelineObject

        try:
            module = importlib.import_module(cls.__module__)
        except ImportError:
            raise ImportError(f"The class {cls} could not be imported.")
        self.cls = cls
        class_default_attrs = getattr(module, "all_default_attrs")
        class_complex_attrs = getattr(module, "complex_attrs_list")

        if cls in DataObject.__subclasses__():
            parent_default_attrs = d_default_attrs
            parent_complex_attrs = d_complex_attrs
        elif cls in PipelineObject.__subclasses__():
            parent_default_attrs = p_default_attrs
            parent_complex_attrs = p_complex_attrs

        self.default_attrs = {**ro_default_attrs, **parent_default_attrs, **class_default_attrs}
        self.complex_attrs = ro_complex_attrs + parent_complex_attrs + class_complex_attrs