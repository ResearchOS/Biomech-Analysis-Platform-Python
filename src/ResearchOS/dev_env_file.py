# If this file exists in this distribution, then the config assumes this is a development or test environment.
# To make it a Production environment, delete this file (done automatically in pyproject.toml)
# If testing, then the environment variable will have already been set.
import os
def set_dev_env():
    if os.environ.get("ENV") != "test":
        os.environ["ENV"] = "dev"