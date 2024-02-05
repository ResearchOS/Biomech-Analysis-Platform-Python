"""Responsible for managing the connection to the database."""

import sqlite3

class Connection():

    def __init__(self, db_file) -> None:
        """Initialize the database connection."""
        self.conn = sqlite3.connect(db_file)

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def commit(self, commit: bool = True) -> None:
        """Commit the transaction."""
        if commit:
            self.conn.commit()

    def cursor(self) -> sqlite3.Cursor:
        """Return a cursor."""
        return self.conn.cursor()