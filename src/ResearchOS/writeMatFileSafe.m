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