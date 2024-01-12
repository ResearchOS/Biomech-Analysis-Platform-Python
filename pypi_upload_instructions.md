From here: https://packaging.python.org/en/latest/tutorials/packaging-projects/
1. python3 -m pip install --upgrade build
2. python3 -m build
3. python3 -m pip install --upgrade twine

All actions are performed in the command line from the root directory of the project.

# Test PyPI
## Upload
1. python3 -m twine upload --repository testpypi dist/*
## Install
2. python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps ResearchOS

# PyPI
## Upload
1. python3 -m twine upload dist/*
## Install
2. python3 -m pip install ResearchOS