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