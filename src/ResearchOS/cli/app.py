import os

import typer

from ResearchOS.config import Config
from ResearchOS.cli.quickstart import create_folders
from ResearchOS.db_initializer import DBInitializer

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
        create_folders(folder, folders = ["data"], files = ["paths.py", ".gitignore", "src..research_objects.dataset.py", "src..research_objects.logsheets.py"])
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
        import_code(repo)
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
def import_code(url: str):
    typer.echo(f"Downloading {url}")

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

if __name__ == "__main__":
    app()
