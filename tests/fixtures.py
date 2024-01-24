from ResearchOS.action import Action
from ResearchOS.config import Config

import sqlite3


try:
    # When running with pytest.
    import pytest
    @pytest.fixture(scope="session")
    def db_conn():
        config = Config()
        Action._db_file = config.db_file
        conn = sqlite3.connect(config.db_file)
        Action.conn = conn
        yield conn
        conn.close()
except ImportError:
    # When running manually.
    def db_conn():
        config = Config()
        Action._db_file = config.db_file
        conn = sqlite3.connect(config.db_file)
        Action.conn = conn
        yield conn
        conn.close()
            



