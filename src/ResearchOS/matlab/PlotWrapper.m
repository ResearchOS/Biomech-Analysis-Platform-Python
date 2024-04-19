function [fig] = PlotWrapper(func_name, save_path, varargin)

%% PURPOSE: WRAPPER FUNCTION TO PLOT DATA

v = varargin{1};

% Determine whether I need to drop the node_info
func_handle = str2func(func_name);
ninputs = nargin(func_handle);
if ninputs<length(v)
    v(end) = [];
end

fig = feval(func_name, v{:}); % Run again without the node info.

% Save the figure
saveas(fig, save_path, 'fig');
saveas(fig, save_path, 'png');
saveas(fig, save_path, 'svg');

close(fig);

end