from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.action import Action

# from sql_joiner_most_recent import col_names_in_where, extract_sql_components, append_table_to_columns
from ResearchOS.sql.sql_joiner_most_recent import extract_sql_components
from ResearchOS.current_user import CurrentUser

def sql_order_result(action: "Action", sqlquery: str, unique_cols: list, single: bool = True, user: bool = True, computer: bool = True) -> None:
    """Takes in a basic SQL query, parses it, and returns the rows of that table ordered by the datetime column of the Actions table
    If "single" is True, returns only the single most recent value for each unique combination of WHERE conditions."""
    
    # Get the action_id's of the current user & computer in the Actions table.
    current_user = CurrentUser(action)
    current_user_timestamps = current_user.get_timestamps_when_current(user = user, computer = computer)

    # FOR THE QUERY THAT DOES NOT LIMIT THE NUMBER OF ROWS
    sqlquery_inner = "SELECT action_id, datetime FROM actions WHERE "
    placeholders = []
    # params = []
    # cursor = action.conn.cursor()
    for start, end in current_user_timestamps:
        placeholders.append(f"(datetime >= '{start}' AND datetime < '{end}')") # Inclusive, exclusive.
        # params.extend([start, end])
    sqlquery_inner += " OR ".join(placeholders)    

    # Extract the components of the SQL query
    columns, table, where_criteria = extract_sql_components(sqlquery)
    columns_w_table = ", ".join([f"{table}.{col}" for col in columns])
    unique_cols_w_table = ", ".join([f"{table}.{col}" for col in unique_cols])

    # sqlquery_final = "SELECT " + columns_w_table + " FROM (" + sqlquery_inner + ") AS current_user_actions JOIN " + table + " ON current_user_actions.action_id = " + table + ".action_id WHERE " + where_criteria + " ORDER BY current_user_actions.datetime DESC"

    columns_w_result_table = ", ".join([f"result.{col}" for col in columns])
    sqlquery_inner2 = "SELECT " + columns_w_table + ", ROW_NUMBER() OVER (PARTITION BY " + unique_cols_w_table + " ORDER BY current_user_actions.datetime DESC) AS row_num FROM (" + sqlquery_inner + ") AS current_user_actions JOIN " + table + " ON current_user_actions.action_id = " + table + ".action_id WHERE " + where_criteria

    sqlquery_final2 = "SELECT " + columns_w_result_table + " FROM (" + sqlquery_inner2 + ") AS result WHERE result.row_num = 1;"

    # Define the ORDER query
    # table_cols = ", ".join([f"{table}.{col}" for col in columns])
    # order_query = f"SELECT {table_cols} FROM {table} JOIN actions ON {table}.action_id = actions.action_id WHERE {where_criteria} ORDER BY actions.datetime DESC"

    return sqlquery_final2