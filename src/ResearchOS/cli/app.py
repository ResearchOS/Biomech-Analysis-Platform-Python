# cli.py
import os

import typer

from ResearchOS.config import Config
from ResearchOS.cli.quickstart import create_folders
from ResearchOS.db_initializer import DBInitializer

app = typer.Typer()

cwd = os.getcwd()

@app.command()
def quickstart(folder: str = typer.Option(cwd, help="Folder name"),
               url: str = typer.Option(None, help="URL to import code from")
               ):
    create_folders(folder)
    db = DBInitializer() # Create the databases.
    if url:
        import_code(url)
    # TODO: Create a .gitignore file.

@app.command()
def import_code(url: str):
    typer.echo(f"Downloading {url}")

@app.command()
def config(github_token: str = typer.Option(None, help="GitHub token"),
           db_file: str = typer.Option("researchos.db", help="Database file"),
           data_db_file: str = typer.Option("researchos.db", help="Data database file"),
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
