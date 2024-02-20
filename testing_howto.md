A great YouTube video on the topic: https://www.youtube.com/watch?v=DhUpxWjOhME

# For consistent import statements in test & dev:
pip install -e .

# For setting up pytest fixtures & debugging during tests:
pip install pytest
Make sure there is a "conftest.py" file in the tests directory.
In that file, inside of the __name__ == "__main__" block, add the following: pytest.main(["-v", "tests"])