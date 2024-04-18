import os, sys
import importlib
import json
import time
import copy

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
from ResearchOS.sql.sql_runner import sql_order_result

app = typer.Typer()

def add_src_to_path():
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
    add_src_to_path()
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
    add_src_to_path()
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
    add_src_to_path()
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
    import matplotlib.pyplot as plt
    from netgraph import Graph, InteractiveGraph, EditableGraph
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
        result = [row for row in result if set(path).issubset(json.loads(row[2]))]
    
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

@app.command()
def logsheet_read():
    """Run the logsheet."""
    add_src_to_path()
    tomlhandler = TOMLHandler("pyproject.toml")
    dataset_raw_path = tomlhandler.toml_dict["tool"]["researchos"]["paths"]["research_objects"]["dataset"]    
    dataset_py_path = tomlhandler.make_abs_path(dataset_raw_path)
    dataset_py_path = dataset_py_path.replace("/", os.sep)
    dataset_py_path = dataset_py_path.replace(os.sep, ".").replace(".py", "")

    logsheet_raw_path = tomlhandler.toml_dict["tool"]["researchos"]["paths"]["research_objects"]["logsheet"]
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
    add_src_to_path()
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
    cls = [cls for cls in subclasses if cls.__name__.lower() == ro_type.lower()]
    if len(cls) == 0:        
        cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.lower() == ro_type.lower()]
    if len(cls) == 0:
        print(f"Research object type {ro_type} not found.")
        return        
    ro_type = cls[0]
    # Get the pyproject.toml file path from all the places it could be.
    # TODO: Support more than "venv" here.
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
    
    toml_handler = TOMLHandler(toml_path)

    paths = toml_handler.get("tool.researchos.paths.research_objects." + ro_name)
    root = toml_handler.get("tool.researchos.paths.root.root")

    if not isinstance(paths, list):
        paths = [paths]

    if not os.path.isabs(root):
        root = os.path.join(os.getcwd(), root)
    
    for path in paths:
        full_path = os.path.join(root, path)
        command = "code " + full_path
        os.system(command)

@app.command()
def show_pl(plobj_id: str = typer.Argument(help="Pipeline object ID", default=None)):
    """Show the pipeline objects in the graph in topologically sorted order."""
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.DataObjects.dataset import Dataset
    from ResearchOS.build_pl import build_pl
    from ResearchOS.build_pl import import_objects_of_type
    add_src_to_path()
    print('Importing Dataset objects...')
    import_objects_of_type(Dataset)
    print('Importing Logsheet objects...')
    lgs = import_objects_of_type(Logsheet)
    lg = lgs[0]
    # Build my pipeline object MultiDiGraph. Nodes are Logsheet/Process objects, edges are "Connection" objects which contain the VR object/value.          
    G = build_pl()
    action = Action(name = "run_pipeline", type="run")
    try:
        pass
    except Exception as e:
        print(f"Error building the pipeline: {e}")

    if plobj_id is None:
        lg_id = None
        plobj_id = lg_id
    
    plobj = None
    for plobj_tmp in G.nodes():
        if plobj_tmp.id == plobj_id: 
            plobj = plobj_tmp           
            break

    if plobj is None and plobj_id is not None:
        raise ValueError("Pipeline object not found.")

    # Check if the previous nodes are all up to date.
    if plobj:
        anc_nodes = list(nx.ancestors(G, plobj))
    else:
        anc_nodes = list(G.nodes())
        print('Starting at the root node of the Pipeline!')
    anc_graph = G.subgraph(anc_nodes)
    anc_nodes_sorted = list(nx.topological_sort(anc_graph))
    if not anc_nodes_sorted:
        print('Nothing to run! Aborting...')
        return
        
    # Get the date last edited for each pipeline object, and see if it was settings or run action.
    sqlquery_raw = "SELECT action_id_num, pl_object_id FROM run_history WHERE pl_object_id IN ({})".format(",".join(["?" for i in range(len(anc_nodes_sorted))]))
    sqlquery = sql_order_result(action, sqlquery_raw, ["pl_object_id", "action_id_num"], single = False, user = True, computer = False)
    anc_node_ids = [node.id for node in anc_nodes_sorted]    
    params = tuple(anc_node_ids)
    run_history_result = action.conn.cursor().execute(sqlquery, params).fetchall()    

    if not run_history_result:
        print("No run history found. Need to run the logsheet before running the pipeline. Attempting to run the logsheet for you...")
        time.sleep(2)
        try:            
            lg.read_logsheet()
            run_history_result = action.conn.cursor().execute(sqlquery, params).fetchall()    
        except:
            raise ValueError("Error auto-running logsheet. Must be addressed before the pipeline can be run.")
        
    run_history_result = [(r + ("run",)) for r in run_history_result]

    sqlquery_raw = "SELECT action_id_num, object_id FROM simple_attributes WHERE object_id IN ({})".format(",".join(["?" for i in range(len(anc_nodes_sorted))]))
    sqlquery = sql_order_result(action, sqlquery_raw, ["action_id_num", "object_id"], single = False, user = True, computer = False)
    settings_history_result = action.conn.cursor().execute(sqlquery, params).fetchall()
    settings_history_result = [(r + ("settings",)) for r in settings_history_result]
    result = run_history_result + settings_history_result
    action_id_nums = list(set([r[0] for r in result]))

    # Get the dates for action_ids
    sqlquery_raw = "SELECT action_id_num, datetime FROM actions WHERE action_id_num IN ({})".format(",".join(["?" for i in range(len(action_id_nums))]))
    sqlquery = sql_order_result(action, sqlquery_raw, ["action_id_num"], single = False, user = True, computer = False)
    params = tuple(action_id_nums)
    action_dates = action.conn.cursor().execute(sqlquery, params).fetchall()
    action_dates_dict = {r[0]: r[1] for r in action_dates}
    result = [(r + (action_dates_dict[r[0]],)) for r in result]
    result = sorted(result, key = lambda x: x[3]) # Sorted by datetime, descending.
    first_out_of_date = []
    for anc_node in anc_nodes_sorted:
        if isinstance(anc_node, Logsheet):
            continue # Can't run the pipeline from a Logsheet.
        most_recent_idx = [idx for idx, r in enumerate(result) if r[1] == anc_node.id][-1]
        if result[most_recent_idx][2] == "run":
            continue
        else:
            # Ensure that there are multiple potential root nodes, to handle the case where maybe multiple nodes of the same generation are out of date.
            first_out_of_date.append(anc_node)
            for out_of_date_node in first_out_of_date:
                if anc_node in nx.descendants(G, out_of_date_node):                    
                    first_out_of_date.remove(anc_node)
                    break

    if first_out_of_date:
        pass
        # print(f"Node {first_out_of_date.id} is out of date. Running the pipeline starting from this node.")        
    else:
        if not plobj:
            print('All nodes are up to date. Nothing to run.')
            return
        print('All previous nodes are up to date. Run starting from specified node.')        
        first_out_of_date = plobj

    run_nodes = copy.copy(first_out_of_date)
    for node in first_out_of_date:
        run_nodes.extend(list(nx.descendants(G, node)))

    run_nodes_graph = G.subgraph(run_nodes)

    up_to_date_nodes = [n for n in G.nodes() if n not in run_nodes]
    up_to_date_nodes_graph = G.subgraph(up_to_date_nodes)
    up_to_date_nodes_sorted = list(nx.topological_sort(up_to_date_nodes_graph))
    print("Up to date nodes:")
    for idx, pl_node in enumerate(up_to_date_nodes_sorted):
        print(str(idx) + ":", pl_node.id)

    run_nodes_sorted = list(nx.topological_sort(run_nodes_graph))
    print("Run nodes:")
    for idx, pl_node in enumerate(run_nodes_sorted):
        print(str(idx) + ":", pl_node.id)

    return run_nodes_sorted, up_to_date_nodes_sorted

@app.command()
def run(plobj_id: str = typer.Argument(help="Pipeline object ID", default=None),
        yes_or_no: bool = typer.Option(False, "--yes", "-y", help="Type '-y' to run the pipeline without confirmation.")):
    """Run the runnable pipeline objects."""
    from ResearchOS.PipelineObjects.logsheet import Logsheet    
    
    run_nodes_sorted, up_to_date_nodes_sorted = show_pl(plobj_id)
    action = Action(name = "run_pipeline", type="run")

    dur = 0
    result = ""
    if yes_or_no != "y":
        result = input_with_timeout(f"Press Enter to continue, or any other key to cancel. Or auto-start in {dur} seconds.", dur)        
    if result == "":
        pass # No user input, or hit enter.
    else:
        print('Pipeline run cancelled.')
        return
            
    for pl_node in run_nodes_sorted:
        if isinstance(pl_node, Logsheet):
            continue
        pl_node.run(action=action, return_conn=False)

def input_with_timeout(prompt, timeout):
    import threading
    result = [""]

    def wait_for_input():
        result[0] = input(prompt)

    thread = threading.Thread(target=wait_for_input)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        print('Starting the pipeline...')

    return result[0]

@app.command()
def init_bridges():
    """Creates the bridges.py file in the project root directory.
    If the file already exists, does not do anything."""
    from ResearchOS.build_pl import build_pl
    from ResearchOS.build_pl import import_objects_of_type
    tomlhandler = TOMLHandler("pyproject.toml")
    # Respects the root folder.
    bridges_path = tomlhandler.get("tool.researchos.paths.root.bridges") 
    bridges_path = tomlhandler.make_abs_path(bridges_path)   
    if os.path.exists(bridges_path):
        print("Bridges file already exists. Aborting...")
        return
    
    # 1. Build the pipeline.
    G = build_pl()

    # 2. Get the most up-to-date inputs and outputs where show = True.
    sqlquery_raw = "SELECT id FROM inputs_outputs WHERE show = ?"
    action = Action(name = "init_bridges")
    params = (1,)
    sqlquery = sql_order_result(action, sqlquery_raw, ["show"], single = False, user = True, computer = False)
    result = action.conn.cursor().execute(sqlquery, params).fetchall()
    put_ids = [r[0] for r in result]

    # 3. Look at lets_puts table to see which inlets/outlest are connected to those inputs/outputs.
    sqlquery_raw = "SELECT let_id, put_id FROM lets_puts WHERE put_id IN ({})".format(",".join(["?" for i in range(len(put_ids))]))
    sqlquery = sql_order_result(action, sqlquery_raw, ["put_id"], single = False, user = True, computer = False)
    params = tuple(put_ids)
    result = action.conn.cursor().execute(sqlquery, params).fetchall()
    let_put_ids = [(r[0], r[1]) for r in result]
    let_ids = [r[0] for r in let_put_ids]

    # 4. Get the inlets/outlets vr_name_in_code and the source pr_id.
    sqlquery_raw = "SELECT id, vr_name_in_code, source_pr_id FROM inlets_outlets WHERE id IN ({})".format(",".join(["?" for i in range(len(let_ids))]))
    sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single = False, user = True, computer = False)
    params = tuple(let_ids)
    result = action.conn.cursor().execute(sqlquery, params).fetchall()
    let_ids = [r[0] for r in result]
    vr_name_in_codes = [r[1] for r in result]
    source_pr_ids = [r[2] for r in result]

    # 5. Look in each loaded module under each package's Process field for the variable name in the code.
    # 5.1. Get the package names.
    packages_folder = os.path.join(os.getcwd(), "packages")
    packages = os.listdir(packages_folder)
    # 5.2. Get the process names for each package by scanning each module with dir()
    # Is this right? No idea. Thanks Copilot.
    package_process_dict = {}
    for package in packages:
        package_path = os.path.join(packages_folder, package)
        if not os.path.isdir(package_path):
            continue
        package_process_dict[package] = []
        for root, dirs, files in os.walk(package_path):
            for file in files:
                if file.endswith(".py"):
                    module_path = os.path.join(root, file)
                    module_path = module_path.replace("/", os.sep)
                    module_path = module_path.replace(os.sep, ".").replace(".py", "")
                    module = importlib.import_module(module_path)
                    process_names = [obj for obj in dir(module) if obj.startswith("PR")]
                    package_process_dict[package].extend(process_names)        

    # 6. Compose each line of the bridges.py file as: 
    # <package_abbrev>.<process_name>[<variable_name_in_code>] = None

    # 7. Write the file. Organize the rows by packages, then by process name organized topologically.

    

if __name__ == "__main__":
    app(["run"])  
    # app(["show-pl"])
    # app(["db-reset","-y"])
    # app(["logsheet-read"])  
