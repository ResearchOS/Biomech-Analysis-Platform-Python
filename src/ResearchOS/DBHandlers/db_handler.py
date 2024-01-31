

class DBHandler():
    """Handle database operations."""

    def _get_time_ordered_result(unordered_result, action_col_num):
        """Return the result ordered by time."""
        return sorted(unordered_result, key=lambda x: x[action_col_num])
    
    def _get_attr_name(attr_id):
        """Return the attribute name."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_name FROM attributes WHERE attr_id = '{attr_id}'"
        result = cursor.execute(sqlquery).fetchone()
        return result[0]