function [a] = PlotWrapper(func_name, save_path, varargin)

%% PURPOSE: WRAPPER FUNCTION TO PLOT DATA

persistent fig;

if isempty(fig)
    fig = figure();         
end

if ~isvalid(fig)
    fig = figure();
end  

clf(fig,'reset');

v = varargin{1};

% Determine whether I need to drop the node_info
func_handle = str2func(func_name);
ninputs = nargin(func_handle);
if ninputs<length(v)+1 % +1 for the fig.
    v(end) = [];
end

v = [{fig}, v]; % Put the figure as the first input always.

feval(func_name, v{:});

% Save the figure
saveas(fig, save_path, 'fig');
saveas(fig, save_path, 'png');
saveas(fig, save_path, 'svg');

a = NaN;

end