import os
import importlib

import typer
from typer.testing import CliRunner

from ResearchOS.config import Config
from ResearchOS.cli.quickstart import create_folders
from ResearchOS.db_initializer import DBInitializer
from ResearchOS.action import Action

app = typer.Typer()

@app.command()
def init_project(folder: str = typer.Option(None, help="Folder name"),
               repo: str = typer.Option(None, help="Repository URL to clone code from")
               ):
    """Create a new blank project in the specified folder. If no arguments are provided, creates a new project in the current working directory.
    If a URL is provided, clones the repository specified by the URL."""
    cwd = os.getcwd()
    if not folder:
        folder = cwd
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except FileNotFoundError:
            raise ValueError(f"Folder {folder} is not a valid folder path.")
    create_folders(folder)
    # If this is a project (because the current working directory and the folder name match) create project-specific folders & files.
    if folder == cwd:
        create_folders(folder, folders = ["data", "output", "output.plots", "output.stats"], files = ["paths.py", ".gitignore", "src..research_objects.dataset.py", "src..research_objects.logsheets.py"])
    config = Config()
    user_input = "y"
    if os.path.exists(config.db_file) or os.path.exists(config.data_db_file):
        user_input = input("Databases already exist. Do you want to overwrite them? (y/n) ")
    if user_input.lower() == "y":
        db = DBInitializer() # Create the databases.
        print("Databases reset to default state.")
    else:
        print("Databases not modified.")
    if repo:
        pass
    print(f"Project initialized in {folder}")

@app.command()
def init_package(subfolder: str = typer.Argument(help="Subfolder to create the package in. Can be relative or absolute path")):
    """Initialize the folder structure to create a package. Must provide the subfolder to create the package in."""
    if not os.path.exists(subfolder):
        try:
            os.makedirs(subfolder)
        except FileNotFoundError:
            raise ValueError(f"Folder {subfolder} is not a valid folder path.")
    create_folders(subfolder)
    print(f"Package initialized in {subfolder}")

@app.command()
def config(github_token: str = typer.Option(None, help="GitHub token"),
           db_file: str = typer.Option("researchos.db", help="Database file"),
           data_db_file: str = typer.Option("researchos_data.db", help="Data database file"),
           data_objects_path: str = typer.Option("research_objects.data_objects.py", help="Path to data objects")
        ):
    config = Config()
    if github_token:
        config.github_token = github_token
    if db_file:
        config.db_file = db_file
    if data_db_file:
        config.data_db_file = data_db_file
    if data_objects_path:
        config.data_objects_path = data_objects_path

@app.command()
def dobjs(is_active: int = typer.Option(1, help="1 for active, 0 for inactive, default 1")):
    """List all available data objects."""
    action = Action(name = "list_dobjs")
    cursor = action.conn.cursor()
    sqlquery = "SELECT path_id, dataobject_id, path FROM paths WHERE is_active = ?"
    params = (is_active,)
    result = cursor.execute(sqlquery, params).fetchall()    
    max_num_levels = -1
    str_lens = []
    for row in result:
        str_lens_in_row = []
        for col in row:
            curr_len = len(str(col))
            str_lens_in_row.append(curr_len)
            max_num_levels = max(max_num_levels, curr_len)
        str_lens.append(str_lens_in_row)

    lens = [-1 for i in range(max_num_levels)]
    for i in range(max_num_levels):
        lens[i] = max([row[i] for row in str_lens if len(row)>=i+1])
    
    for row in result:
        row = row.append([""]*(max_num_levels-len(row)))
        print("{:<12} {:<15} {:<5}".format(f"Path ID: {row[0]}", f"DataObject ID: {row[1]}", f"Path: {row[2]}"))

@app.command()
def db_reset():
    """Reset the databases to their default state."""
    # Ask the user for confirmation.
    user_input = input("Are you sure you want to reset the databases to their default (empty) state? All data will be deleted! (y/n) ")
    if user_input.lower() != "y":
        print("Databases not modified.")
        return
    db = DBInitializer()
    print("Databases reset to default state.")

@app.command()
def logsheet_read(path: str = typer.Argument(help="Path to the logsheet research object file. Default is research_objects.logsheets.py", default="research_objects.logsheets.py")):
    """Run the logsheet."""
    lgs = importlib.import_module(path)
    lg_objs = [lg_obj for lg_obj in dir(lgs) if hasattr(lg_obj,"prefix") and lg_obj.prefix == "LG"]
    lg_obj = lg_objs[0]
    lg_obj.read_logsheet()

if __name__ == "__main__":
    pass
    # app("logsheet-read","C:\\Users\\Mitchell\\Desktop\\Matlab Code\\GitRepos\\CAREER-SLG-SPEED\\research_objects\\logsheets.py")
