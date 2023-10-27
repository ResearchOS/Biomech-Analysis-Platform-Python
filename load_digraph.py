import networkx as nx
import sqlite3

def load_digraph(db_file: str) -> nx.DiGraph:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    graph = nx.DiGraph()

    # Get all of the table names
    cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
    table_names = cursor.fetchall()

    # Get the node tables
    # Get all of the table_names that contain the string "Instances"
    node_tables = []
    for table in table_names:
        if "Instances" in table[0]:
            node_tables.append(table[0])
    

    # Get all of the nodes from each of the tables        
    for table in node_tables:
        cursor.execute(f"SELECT * FROM {table}")
        nodes_result = cursor.fetchall() # Returns a tuple per row, UUID is the first element.
        columns = [description[0] for description in cursor.description]
        for node in nodes_result:            
            graph.add_node(node[0])
            for i, column in enumerate(columns[1:]):
                graph.nodes[node[0]][column] = node[i+1]

    # Get the edge tables. These are all of the remaining tables in table_names not prsent in node_tables
    settings_tables = ["Settings", "Users", "PR_ST_AN"] # Need to incorporate ST_PR_AN at some point.
    edge_tables = []
    for table in table_names:
        if "Abstract" in table[0]:
            continue

        if table[0] in node_tables:
            continue

        if table[0] in settings_tables:
            continue
    
        edge_tables.append(table[0])
    
    # Get all of the edges from each of the linkage tables
    for table in edge_tables:
        cursor.execute(f"SELECT * FROM {table}")
        edges_result = cursor.fetchall() # Returns a tuple per row.        
        columns = [description[0] for description in cursor.description]
        source = 1
        target = 0
        if "VR" in table and "PR" in table:
            source = 0
            target = 1

        for edge in edges_result:
            graph.add_edge(edge[source], edge[target])
            for i, column in enumerate(columns[2:]):
                graph.edges[edge[source], edge[target]][column] = edge[i+2]

    return graph

if __name__ == "__main__":
    db_file = "/Users/mitchelltillman/Desktop/Work/MATLAB_Code/GitRepos/Biomech-Analysis-Platform-MATLAB/Databases/biomechOS.db"
    digraph = load_digraph(db_file)
    print(digraph.nodes)
    print(digraph.edges)
    