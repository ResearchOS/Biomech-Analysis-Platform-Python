function [] = safe_save(mat_data_folder, dobj, data_struct_to_save)

%% PURPOSE: SAVE THE OUTPUT VARIABLES FOR THIS DATA OBJECT TO A MAT FILE.

% 1. Concatenate the mat_data_folder with the dobj to get the file path.
% Create the directory if it doesn't already exist.
rel_file_path = strrep(dobj, '.', filesep);
mat_file_path = [mat_data_folder filesep rel_file_path '.mat'];

% 2. Save the file
writeMatFileSafe(mat_file_path, data_struct_to_save);

end

function data = readMatFileSafe(filename, vars_to_load)
% READMATFILESAFE Reads a .mat file in a thread-safe manner
%   data = READMATFILESAFE(filename) reads the contents of the specified
%   .mat file using a lock file to ensure thread safety.

% Generate lock file name
[filepath, name, ~] = fileparts(filename);
lockfile = fullfile(filepath, [name '.lock']);

% Wait for lock
while true
    try
        fileID = fopen(lockfile, 'w');
        if fileID ~= -1            
            fclose(fileID);
            break;
        elseif exist(filepath,'dir') ~= 7
            mkdir(filepath);
        end
    catch ME
        % Lock file exists, wait and retry
        pause(0.1);
    end
end

try
    % Read the .mat file
    data = load(filename, vars_to_load{:});
catch ME
    % Remove lock file
    delete(lockfile);
    rethrow(ME);
end

% Remove lock file
delete(lockfile);
end

function writeMatFileSafe(filename, data)
% WRITEMATFILESAFE Writes data to a .mat file in a thread-safe manner
%   WRITEMATFILESAFE(filename, data) writes the specified data to a .mat
%   file using a lock file to ensure thread safety.

% Generate lock file name
[filepath, name, ~] = fileparts(filename);
lockfile = fullfile(filepath, [name '.lock']);

% Wait for lock    
while true
    try
        fileID = fopen(lockfile, 'w');
        if fileID ~= -1            
            fclose(fileID);
            break;
        elseif exist(filepath,'dir') ~= 7
            mkdir(filepath);
        end
    catch ME        
        % Lock file exists, wait and retry
        pause(0.1);
    end
end

try
    % Write the .mat file
    save(filename, '-struct', 'data','-v6','-append');
catch
    try
        save(filename, '-struct','data','-v6');
    catch ME
        % Remove lock file
        delete(lockfile);
        rethrow(ME);
    end
end

% Remove lock file
delete(lockfile);
end