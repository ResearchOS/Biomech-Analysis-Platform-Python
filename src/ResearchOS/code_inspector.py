import ast, inspect
from typing import Callable

def get_returned_variable_names(func: Callable):
    """
    Extracts the names of variables that are returned by the given function, including
    cases where multiple variables are returned in a single return statement.
    
    :param func: The function to analyze.
    :return: A list of variable names returned by the function.
    """
    if func is None:
        return
    # Get the source code of the function
    func_code = inspect.getsource(func)
    
    # Parse the source code into an AST
    tree = ast.parse(func_code)
    
    # Define a visitor class to collect returned variable names
    class ReturnVisitor(ast.NodeVisitor):
        def __init__(self):
            self.returned_vars = []
        
        def visit_Return(self, node):
            # If the return value is a single variable (ast.Name)
            if isinstance(node.value, ast.Name):
                self.returned_vars.append(node.value.id)
            # If the return value is a tuple (ast.Tuple)
            elif isinstance(node.value, ast.Tuple):
                for elt in node.value.elts:
                    if isinstance(elt, ast.Name):
                        self.returned_vars.append(elt.id)
            # Continue traversing the tree
            self.generic_visit(node)
    
    # Instantiate the visitor and traverse the AST
    visitor = ReturnVisitor()
    visitor.visit(tree)
    
    return visitor.returned_vars

def get_input_variable_names(func: Callable):
    """Get the names of the input variables of a function."""
    if func is None:
        return
    
    sig = inspect.signature(func)
    parameters = sig.parameters

    # Distinguish required vs. optional parameters
    required_params = [p for p in parameters.values() if p.default == p.empty]
    req_param_names = [p.name for p in required_params]
    return req_param_names

# Example function with a single return statement returning multiple values
def example_function():
    x = 1
    y = 2
    z = 3
    if x:
        return x, z
    else:
        return y
    # return x, y, z

if __name__ == "__main__":
    # Extract and print the names of variables returned by the function
    returned_var_names = get_returned_variable_names(example_function)
    print(returned_var_names)
