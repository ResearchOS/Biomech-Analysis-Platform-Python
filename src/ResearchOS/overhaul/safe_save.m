function [] = safe_save(mat_data_folder, dobj, data_struct_to_save)

%% PURPOSE: SAVE THE OUTPUT VARIABLES FOR THIS DATA OBJECT TO A MAT FILE.

% 1. Concatenate the mat_data_folder with the dobj to get the file path.
% Create the directory if it doesn't already exist.
rel_file_path = strrep(dobj, '.', filesep);
mat_file_path = [mat_data_folder filesep rel_file_path '.mat'];

% 2. Save the file
writeMatFileSafe(mat_file_path, data_struct_to_save);

end