# SQLite Database

## Introduction
ResearchOS uses two SQLite databases to store data. The first database is used to store the [Research Objects](../Research%20Objects/research_object.md) that define the structure and maintain the history of your project. The second database is used to store the data itself. These databases are stored in the root of the project folder and are named `researchos.db` and `researchos_data.db`, respectively. Because `researchos_data.db` contains the project's data, and can therefore become quite large, I recommend that it be added to the `.gitignore` file to prevent it from being uploaded to a repository.

## Database Structure
The `researchos.db` database contains the following tables:

- **research_objects**: The reference list of Research Objects. If the Research Object doesn't exist in this table, then it doesn't exist for the project.

- **actions**: The reference list of Actions that have been performed on this project.