# Process

## Import Attributes
One special case for `Process` objects is when first importing data from their native file formats into ResearchOS. To do that, a couple of custom attributes need to be defined for the `Process` object that performs this task.

The code to be run to import the data from the native file format should be defined in the `method` (Python) or `mfunc_name` (MATLAB) of the `Process` object. The code should handle loading only one file format at a time. If multiple file formats need to be imported, multiple `Process` objects should be created with separate functions for each.

### import_file_ext
A string that specifies the file extension of the native file format. For example, if the native file format is a CSV file, the `import_file_ext` attribute should be set to `csv`.

### import_file_vr_name
A string that specifies the name of the variable that will provide the file path of the file to import the data from. With the [Dataset](../Data%20Objects/dataset.md) `schema` (or `file_schema`) and the `Process` object's `import_file_ext` and `import_file_vr_name` attributes, the full file name of the native data file can be constructed and passed as an input argument to the `Process` object's code.

::: src.ResearchOS.PipelineObjects.process.Process

To run a Process with process.run():
1. Create a new Process object, with id specified as a kwarg.
2. Set the level of the Process object as a DataObject subclass.
3. If a MATLAB function is to be run, set self.is_matlab to True.
4. Set the input and output variables.
    a. Use self.set_input_var() and self.set_output_var() to set the input and output variables using kwargs.
    b. Alternatively, assign self.input_vrs and self.output_vrs directly as dicts with keys for the variable names in code, and the Variable objects are the values.
    NOTE: Ensure that all of the variables are pre-existing, and that their vr.name are unique from all other variables in the pipeline.