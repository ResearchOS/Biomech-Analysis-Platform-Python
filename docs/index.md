# Welcome to ResearchOS (pre-alpha version)

This is the documentation for [ResearchOS](https://github.com/ResearchOS/ResearchOS), a Python package for scientific computing.

# Install with pip
```
pip install researchos
```

# Project Description
Scientific computing is currently fractured, with many competing data standards (or lack thereof) and data processing tools that do not have a common way to communicate. ResearchOS provides a generalized framework to perform scientific computing of any kind, in a modular, easily shareable format.

The primary innovation behind ResearchOS is to treat every single piece of the scientific data analysis workflow as an object, complete with ID and metadata. While this incurs some code overhead, the ability to have a standardized way to communicate between different parts of a pipeline and to share and integrate others' pipelines is invaluable, and sorely needed in the scientific computing community.

# Roadmap
## Version 0.1

- [x] Do multiple things with one Action.
- [x] Create research objects, save and load them with attributes
- [x] Create edges between research objects and allow the edges to have their own attributes.
- [ ] Load and save even complex attributes (e.g. list of dicts) with JSON. Right now I'm just using json.loads()/dumps() but I may need something more sophisticated.
- [ ] Implement read logsheet.
    - [ ] Populate the database with the logsheet data.
- [ ] Implement saving participant data to disk/the database.
    - [ ] Implement data schema for participant data
- [ ] Implement subsets.
- [ ] Publish my proof of concept to JOSS.

## Version 0.2
- [ ] Create a graph of research objects and edges
- [ ] Implement rollback-able version history for research objects
- [ ] Enhance multi-user support on the same machine.
- [ ] Look into CI/CD best practices, improve test coverage.
- [ ] Import/export a ResearchObject for sharing with other users.
- [ ] Export stats results to LaTeX tables.
- [ ] Export images to LaTeX figures.
    - [ ] For images with transparent backgrounds, allow them to be stacked so that multiple can be compared at once.

## Version 0.3 and beyond
- [ ] Implement a MariaDB-based backend for ResearchOS so that it can be used in a multi-user environment.
- [ ] Implement password-based authentication for the MariaDB backend.
- [ ] Implement a web-based frontend for ResearchOS.
- [ ] Get journals on board with ResearchOS so that they can accept ResearchObjects with submissions.
- [ ] Integrate ResearchOS with participant management systems like RedHat so that people & data are linked.
