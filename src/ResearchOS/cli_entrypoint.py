import os, sys
import importlib
import json

import typer
from typer.testing import CliRunner
import toml
import networkx as nx

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.config import Config
from ResearchOS.cli.quickstart import create_folders
from ResearchOS.db_initializer import DBInitializer
from ResearchOS.action import Action, logger
from ResearchOS.tomlhandler import TOMLHandler

app = typer.Typer()
# Add the current working directory to the path so that the code can be imported.
sys.path.append(os.getcwd())
with open ("pyproject.toml", "r") as f:
    pyproject = toml.load(f)
src_path = pyproject["tool"]["researchos"]["paths"]["root"]["root"]
if not os.path.isabs(src_path):
    src_path = os.path.join(os.getcwd(), src_path)
sys.path.append(src_path)
logger.warning(f"Adding {src_path} to the path.")


@app.command()
def init_project(folder: str = typer.Option(None, help="Folder name"),
               repo: str = typer.Option(None, help="Repository URL to clone code from")
               ):
    """Create a new blank project in the specified folder. If no arguments are provided, creates a new project in the current working directory.
    If a URL is provided, clones the repository specified by the URL."""
    cwd = os.getcwd()
    if folder is None:
        folder = cwd
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
            logger.info(f"Folder {folder} created.")
        except FileNotFoundError:
            msg = f"Folder {folder} is not a valid folder path."
            logger.error(msg)
            raise ValueError(msg)
    create_folders(folder)
    # If this is a project (because the current working directory and the folder name match) create project-specific folders & files.
    create_folders(folder, folders = ["data", "output", "output/plots", "output/stats", "packages"], files = ["paths.py", ".gitignore", "src/research_objects/dataset.py", "src/research_objects/logsheets.py"])
    config = Config(type="Project")
    user_input = "y"
    if os.path.exists(config.db_file) or os.path.exists(config.data_db_file):
        user_input = input("Databases already exist. Do you want to overwrite them? (y/n) ")
    if user_input.lower() == "y":
        db = DBInitializer() # Create the databases.
        db_msg = "Databases reset to default state."        
    else:
        db_msg = "Databases not modified."
    logger.warning(db_msg)
    if repo:
        pass
    msg = f"Project initialized in {folder}"
    logger.info(msg)

@app.command()
def init_package(name: str = typer.Argument(help="Package name. This will be the name of the folder.")):
    """Initialize the folder structure to create a package. Must provide the subfolder to create the package in."""
    cwd = os.getcwd()
    packages_folder = os.path.join(cwd, "packages")
    package_folder = os.path.join(packages_folder, name)
    if not os.path.exists(package_folder):
        try:
            os.makedirs(package_folder)
        except FileNotFoundError:
            msg = f"Folder {package_folder} is not a valid folder path."
            logger.error(msg)
            raise ValueError(msg)
    create_folders(package_folder)
    logger.info(f"Package folder {package_folder} created.")

@app.command()
def config(github_token: str = typer.Option(None, help="GitHub token"),
           db_file: str = typer.Option(None, help="Database file"),
           data_db_file: str = typer.Option(None, help="Data database file"),
           data_objects_path: str = typer.Option(None, help="Path to data objects")
        ):
    config = Config()
    command = f'code "{config._config_path}"'
    if github_token is None and db_file is None and data_db_file is None and data_objects_path is None:
        os.system(command)
    if github_token:
        config.github_token = github_token
    if db_file:
        config.db_file = db_file
    if data_db_file:
        config.data_db_file = data_db_file
    if data_objects_path:
        config.data_objects_path = data_objects_path

@app.command()
def db_reset(yes_or_no: bool = typer.Option(False, "--yes", "-y", help="Type 'y' to reset the databases to their default state.")):
    """Reset the databases to their default state."""
    # Ask the user for confirmation.
    user_input = "y"
    if not yes_or_no:
        user_input = input("Are you sure you want to reset the databases to their default (empty) state? All data will be deleted! (y/n) ")        
    if user_input.lower() != "y":
        print("Databases not modified.")
        return
    db = DBInitializer()
    db_msg = "Databases reset to default state."
    logger.warning(db_msg)

@app.command()
def dobjs(path = typer.Option(None, "--path", "-p", help="Path to the data object of interest")):
    """List all available data objects."""
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.DataObjects.dataset import Dataset
    from ResearchOS.DataObjects.data_object import load_data_object_classes
    import matplotlib.pyplot as plt
    from netgraph import Graph, InteractiveGraph, EditableGraph
    breakpoint()
    if path is not None and not isinstance(path, list):
        path = [path]
    action = Action(name = "list_dobjs")
    cursor = action.conn.cursor()
    sqlquery = "SELECT path_id, dataobject_id, path FROM paths WHERE is_active = ?"
    is_active = 1
    params = (is_active,)    
    result = cursor.execute(sqlquery, params).fetchall()
    if not result:
        print("No data objects found.")
        return
    if path is not None:
        result = [row for row in result if path in row[2]]
    
    max_num_levels = -1
    str_lens = []
    for row in result:
        str_lens_in_row = []
        max_num_levels = max(max_num_levels, len(row))
        for col in row:
            curr_len = len(str(col))
            str_lens_in_row.append(curr_len)            
        str_lens.append(str_lens_in_row)
    
    lens = [-1 for i in range(max_num_levels)]
    for i in range(max_num_levels):
        lens[i] = max([row[i] for row in str_lens if len(row)>=i+1])
        
    for row in result:
        path = json.loads(row[2])
        lens_list = [f"{{:<{lens[i]}}}" for i in range(len(path))]
        lens_str = " ".join(lens_list)

        # Prepare the complete format string
        style_str = "{:<12} {:<15} {:<7} " + lens_str          
        print(style_str.format(f"Path ID: {row[0]}", f"DataObject ID: {row[1]}", "Path: ", *path))
    
    # load_data_object_classes() # import the data objects.
    # ds = Dataset(id = ResearchObject._get_dataset_id(False))
    # G = ds.get_addresses_graph()

    # G = nx.DiGraph(G)
    # plot_instance = Graph(G)
    # plt.show()

@app.command()
def logsheet_read():
    """Run the logsheet."""
    tomlhandler = TOMLHandler("pyproject.toml")
    dataset_raw_path = tomlhandler.toml_dict["tool"]["researchos"]["paths"]["research_objects"]["Dataset"]    
    dataset_py_path = tomlhandler.make_abs_path(dataset_raw_path)
    dataset_py_path = dataset_py_path.replace("/", os.sep)
    dataset_py_path = dataset_py_path.replace(os.sep, ".").replace(".py", "")

    logsheet_raw_path = tomlhandler.toml_dict["tool"]["researchos"]["paths"]["research_objects"]["Logsheet"]
    logsheet_py_path = tomlhandler.make_abs_path(logsheet_raw_path)
    logsheet_py_path = logsheet_py_path.replace("/", os.sep)
    logsheet_py_path = logsheet_py_path.replace(os.sep, ".").replace(".py", "")

    logger.warning("MODULE_PATH:" + logsheet_py_path)
    lgs = importlib.import_module(logsheet_py_path)
    # dir() returns names of the attributes of the module. So getattr(module, name) gets the object.
    lg_objs = [getattr(lgs, lg_obj) for lg_obj in dir(lgs) if hasattr(getattr(lgs, lg_obj),"prefix") and getattr(lgs, lg_obj).prefix == "LG"]
    lg_obj = lg_objs[0]
    ds = importlib.import_module(dataset_py_path) # Included to ensure the Dataset object is in the database.
    lg_obj.read_logsheet()

@app.command()
def graph_show(package_or_project_name: str = typer.Argument(default=None, help="Name of the package or project to visualize.")):
    """Show the directed graph of Pipeline Objects within one package or project.
    If no argument is entered, look in the current project's database for the objects.
    If a package name is provided, look in the package's code for the objects."""
    if package_or_project_name is None:
        project_name = os.path.basename(os.getcwd())
        package_or_project_name = os.path.basename(os.getcwd())

@app.command()
def edit(ro_type: str = typer.Argument(help="Research object type (e.g. data, logsheet, pipeline)"),
         p: str = typer.Option(None, help="The package name to open the research object from")):
    """Open a research object in the default editor.
    They can come from a few places:
    1. The project: Looks in the root/pyproject.toml file for the research object location.
    2. The package: Looks in root/packages/package_name/pyproject.toml for the research object location. 
    If root/packages/package_name does not exist, searches through the pip installed packages for a package with the name package_name."""
    from ResearchOS.research_object import ResearchObject
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
    cls = [cls for cls in subclasses if cls.__name__.lower() == ro_type.lower()]
    if len(cls) == 0:        
        cls = [cls for cls in subclasses if cls.prefix.lower() == ro_type.lower()]
    if len(cls) == 0:
        print(f"Research object type {ro_type} not found.")
        return        
    ro_type = cls[0]
    # Get the pyproject.toml file path.
    root_path = os.getcwd()
    root_package_path = os.path.join(root_path, "packages", str(p))
    venv_package_path = os.path.join(os.getcwd(), "venv", "lib", "site-packages", str(p))
    # Project
    if os.path.exists(os.path.join(root_path, "pyproject.toml")):
        toml_path = os.path.join(root_path, "pyproject.toml")
    # My package
    elif os.path.exists(os.path.join(root_package_path, "pyproject.toml")):
        toml_path = os.path.join(root_package_path, "pyproject.toml")
    # Venv package
    elif os.path.exists(os.path.join(venv_package_path, "pyproject.toml")):
        toml_path = os.path.join(venv_package_path, "pyproject.toml")

    if not os.path.exists(toml_path):
        raise ValueError(f"pyproject.toml file not found in {root_path} or {root_package_path} or {venv_package_path}")
    
    ro_name = ro_type.__name__.lower()    
    if ro_name == "dataobjects":
        ro_name = ro_name[:4] + ro_name[4].upper() + ro_name[5:]
    ro_name = ro_name[0].upper() + ro_name[1:]
    
    with open(toml_path, "r") as f:
        pyproject = toml.load(f)

    if "tool" not in pyproject:
        raise ValueError(f"No tool section in {toml_path}.")
    
    if "researchos" not in pyproject["tool"]:
        raise ValueError(f"No researchos section in{toml_path} file.")
    
    if "paths" not in pyproject["tool"]["researchos"]:
        raise ValueError(f"No paths section in {toml_path}.")
    
    if "research_objects" not in pyproject["tool"]["researchos"]["paths"]:
        raise ValueError(f"No research_objects section in {toml_path} file.")
    
    if ro_name not in pyproject["tool"]["researchos"]["paths"]["research_objects"]:
        raise ValueError(f"No path for {ro_name} in {toml_path} file.")

    path = pyproject["tool"]["researchos"]["paths"]["research_objects"][ro_name]

    command = "code " + path

    os.system(command)

if __name__ == "__main__":
    # app()
    app(["logsheet-read"])
