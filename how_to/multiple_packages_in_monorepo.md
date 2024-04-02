I'm thinking that to share code I can create a package for each "pipeline" or block of code in a data analysis pipeline that forms a unit. For example, in my data analysis I'd have one pipeline for calibration, one for computing angular momentum, one for computing linear momentum, and one for computing margin of stability.

### NOTE: Alternatively, there could just be one repository for each package. I want to give authors the option.

Each pipeline package should be a subfolder of the project's root folder. That package's subfolder will be structured as though the package subfolder were the root folder. This way, the package can be imported as though it were a standalone package. This means that each package will have its own `__init__.py` and `pyproject.toml` files, and the package will be installed as a package in the project's virtual environment.

Note that the below directory structure contains the project `my_project` at the root, and the package `my_package_1` as a subfolder of the project. Both the project and the package contain `pyproject.toml` and `LICENSE.md` and `README.md`.

## QUESTION: Should the project be installable as a Python package? Or should that be packages only? If someone wants to replicate a project, they can just clone the repository. If someone wants to use a package (part of a project), they can install the package.
### ANSWER: The pyproject.toml file is necessary so that the project's dependencies can be installed. Therefore the project will be installable as a package, though I'd recommend replicating a project by cloning the repository.

For example:
```ASCII
my_project
├── data
├── docs
├── my_package_1
|   ├── docs
│   ├── src
│   |   ├── my_package_1
│   |   |   ├── plot
|   |   |   |   ├── plot_code.py
│   |   |   ├── process
|   |   |   |   ├── process_code.py
│   |   |   ├── research_objects
│   |   |   |   ├── data_objects.py
│   |   |   |   ├── plots.py
│   |   |   |   ├── stats.py
│   |   |   |   ├── processes.py
│   |   |   |   ├── subsets.py
│   |   |   |   ├── logsheets.py
│   |   |   ├── stats
|   |   |   |   ├── stats_code.py
│   |   |   ├── __init__.py
│   |   |   ├── main.py
│   |   |   ├── paths.py
│   |   |   ├── pyproject.toml
│   |   |   ├── LICENSE.md
│   |   |   ├── README.md
│   ├── tests
├── src
│   ├── my_project
│   |   ├── plot
|   |   |   ├── plot_code.py
│   |   ├── process
|   |   |   ├── process_code.py
│   |   |   ├── research_objects
│   |   |   |   ├── data_objects.py
│   |   |   |   ├── plots.py
│   |   |   |   ├── stats.py
│   |   |   |   ├── processes.py
│   |   |   |   ├── subsets.py
│   |   |   |   ├── logsheets.py
│   |   ├── stats
|   |   |   ├── stats_code.py
│   |   ├── __init__.py
│   |   ├── main.py
│   |   ├── paths.py
├── tests
LICENSE.md
README.md
pyproject.toml
.gitignore
```

The `main.py` file is where the order of the pipeline is defined, as well as bridging the inputs and outputs between different packages. The `paths.py` file is where the paths to the data and outputs are defined.