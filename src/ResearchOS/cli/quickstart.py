import os, shutil

def create_folders(root_folder: str = None):
    """Creates the default folder structure in your project directory."""
    # Check if this folder exists. If it does not, raise an error.
    if not os.path.exists(root_folder):
        raise FileNotFoundError(f"Folder {root_folder} does not exist.")
    
    # Create the folders
    folders = [
        "secrets",
        "data",
        "plots",
        "research_objects",
        "code.processes",
        "code.plots",
        "code.stats",
    ]
    for folder in folders:
        folder_path = replace_dots_with_sep(folder, is_folder=True)        
        os.makedirs(os.path.join(root_folder, folder_path), exist_ok=True)

    files = [
        "secrets.github_api_token.py",
        "research_objects.data_objects.py",
        "research_objects.datasets.py",
        "research_objects.logsheets.py",
        "research_objects.plots.py",
        "research_objects.subsets.py",
        "research_objects.processes.py",
        "research_objects.stats.py",
        "research_objects.variables.py"
    ]
    cli_root = os.path.dirname(os.path.abspath(__file__))
    quickstart_root = os.path.join(cli_root, "quickstart_files")
    for file in files:
        rel_file_path = replace_dots_with_sep(file, is_folder=False) # Replace dots with os.sep
        dest_file_path = os.path.join(root_folder, rel_file_path) # Make destination absolute path
        src_file_path = os.path.join(quickstart_root, rel_file_path) # Make source absolute path
        if os.path.exists(dest_file_path):
            continue # Don't overwrite an existing file.
        if os.path.exists(src_file_path):
            shutil.copyfile(src_file_path, dest_file_path)
        else:
            with open(dest_file_path, "w") as f:
                f.write("")

# Need to parse the file to replace all dots except for the last one with the os.sep
def replace_dots_with_sep(file: str, is_folder: bool):
    """Replace all dots in a file path with the os.sep except for the last dot because it's for the extension."""
    dot_indices = [idx for idx, char in enumerate(file) if char == "."]
    file_as_list = list(file)
    if is_folder:
        use_dot_indices = dot_indices
    else:
        use_dot_indices = dot_indices[:-1]
    for idx in use_dot_indices:
        file_as_list[idx] = os.sep
    return ''.join(file_as_list)