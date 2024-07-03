function [] = wrapper(inputs_metadata, outputs_metadata, mat_file_path)

% Load the input data
input_names = cell(size(inputs_metadata));
input_hashes = cell(size(input_names));
input_values = cell(size(input_names));
for i = 1:length(inputs_metadata)
    input_names{i} = inputs_metadata{i}.name;
    input_hashes{i} = inputs_metadata{i}.hash;
    if isnan(input_hashes{i})
        input_values{i} = inputs_metadata{i}.value;
    end
end

% Load from file.
loaded_inputs = load(mat_file_path, input_hashes{i});
count = 0;
for i = 1:length(inputs_metadata)
    if ~isnan(input_hashes{i}) && isempty(input_values{i})
        count = count + 1;
        input_values{i} = loaded_inputs{count};
    end
end

% Run the function
output_values = feval(fcn, input_values{:});

% Save the output data
outputs_struct = struct;
output_names = fieldnames(outputs_metadata);
output_hashes = cell(size(output_names));
for i = 1:length(output_names)
    output_hashes{i} = outputs_metadata.(output_names{i});
    outputs_struct.(output_hashes{i}) = output_values{i};
end

save(mat_file_path, 'outputs_struct', '-struct','-v6');
