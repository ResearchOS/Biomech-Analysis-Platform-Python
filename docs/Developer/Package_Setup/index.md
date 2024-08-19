Packages Overview

# Terminology
Package: A folder called `<name>` generated from `ros create <name>` with the following folder structure. 

!!! tip "Package Names"
    The package name must begin with `ros-` otherwise creating the package folder will fail.

Project: A package folder in combination with a dataset to perform data analysis.

Dataset: A logsheet & data files.

The dependencies for a specific package are specified in the package's `pyproject.toml` file, in the `dependencies` table, see below for a minimal working example:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.researchos]
index = 'index.toml'

[tool.hatch.build.targets.wheel]
packages = ["src"]

[project]
name = "test-project"
version = "0.0.1"
requires-python = ">=3.7"
authors = [
    {name = "Mitchell Tillman <my.email@gmail.com>"},
]
description = "Example Package"
readme = "README.md"
dependencies = [
    "ros-example-package"
]
```

This tells `pip` that the `ros-example-package` and all of its dependencies need to be installed when installing the `test-project` package.