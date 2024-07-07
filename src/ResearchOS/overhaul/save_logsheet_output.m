function [] = save_logsheet_output(mat_data_folder, schema, data_objects)

%% PURPOSE: SAVE THE SCHEMA & DATA OBJECTS LIST PRODUCED BY THE LOGSHEET.

% 1. Get the file path
file_path = [mat_data_folder 'logsheet_output.mat'];

% 2. Save the file
data.schema = schema;
data.data_objects = data_objects;
writeMatFileSafe(file_path, data);

end
