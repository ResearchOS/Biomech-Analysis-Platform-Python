[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=for-the-badge&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)
[![Coverage Status](https://coveralls.io/repos/github/ResearchOS/ResearchOS/badge.svg?branch=main)](https://coveralls.io/github/ResearchOS/ResearchOS?branch=main)

Documentation found at ResearchOS's [mkdocs page hosted on GitHub Pages](https://researchos.github.io/ResearchOS/)

Please note that this is currently in active development, in pre-alpha as of 1/11/24.

As of 09/28/2024, I decided to split the project across separate repositories. This repository will be the main repo that contains the pip installable package for the ResearchOS CLI and backend system. The DAG compilation now lives in the [ResearchOS/Package-DAG-Compiler](https://github.com/ResearchOS/Package-DAG-Compiler) repository. Reading a .csv file to construct a dataset will live in another repository in the future once it's created. Also, code to actually run the functions will be in a fourth repository (which of course will depend on the first two repositories). This is to make the project more modular and easier to maintain.