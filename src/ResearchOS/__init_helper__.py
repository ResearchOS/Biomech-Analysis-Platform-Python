import os

def set_env():
    """Set the environment to dev."""
    try:
        from .dev_env_file import set_dev_env
        set_dev_env()    
    except ModuleNotFoundError:
        os.environ["ENV"] = "prod"

set_env()