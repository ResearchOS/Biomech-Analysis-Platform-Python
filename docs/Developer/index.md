This is the internal developer documentation.

Every project created using this software is output as a `pip install`able package by default using `>ros create <name>`.

This software is intended for anyone who works with data, from researchers to developers to data scientists. It aims to facilitate sharing code with others, and utilize others' code.

There are three main ways that this software is intended to be used:

1. As a researcher coding your own data analysis, developing new algorithms or data visualizations. Your project, which includes code and data, is organized into a package.

2. As a developer creating a new package for others to use. Typically, there is little or no data associated with these packages.

3. As the project matures and packages that provide common methodologies come to be commonly used, many or most users may simply be consumers of pre-existing pipelines, utilizing their own or others' data, and uniquely combining packages into new analyses, which in turn are packaged to be shared with others.

# Developer Index
## [Packages](../Developer/Package_Setup/index.md)
Describes how the package dependencies are specified and organized.

## [Compilation](../Developer/Compilation/index.md)
Describes how the Directed Acyclic Graph (DAG) is compiled from the .toml settings files across packages.

## [Running](../Developer/Running/index.md)
Describes the process of running the code on the specified dataset.