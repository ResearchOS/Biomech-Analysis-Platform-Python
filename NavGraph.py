import networkx as nx

class NavGraph(nx.DiGraph):
    def __init__(self):
        super().__init__()        

    def build(fcn: callable, db_file: str) -> nx.DiGraph:
        # called_str = fcn + '("' + db_file + '")'
        return fcn(db_file)
    
def test(db_file: str) -> nx.DiGraph:
    pass

if __name__ == "__main__":
    NavGraph.build(test, 'foo')