import sys
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QGraphicsScene, QGraphicsView
from PySide6.QtGui import QPainter, QPen
import networkx as nx

class GraphViewer(QGraphicsView):
    def __init__(self, graph):
        super(GraphViewer, self).__init__()
        self.graph = graph
        self.setWindowTitle('NetworkX Graph Viewer')
        self.setScene(QGraphicsScene())
        self.setSceneRect(0, 0, 800, 600)
        
    def paintEvent(self, event):
        painter = QPainter(self.viewport())
        painter.setRenderHint(QPainter.Antialiasing)
        
        node_radius = 20
        node_color = '#FF0000'
        edge_color = '#0000FF'
        
        pen = QPen(Qt.black)
        pen.setWidthF(2.0)
        painter.setPen(pen)
        
        for node in self.graph.nodes:
            x, y = self.graph.nodes[node]['pos']
            painter.setBrush(Qt.red)
            painter.drawEllipse(x - node_radius, y - node_radius, node_radius * 2, node_radius * 2)
            painter.drawText(x - node_radius // 2, y + node_radius // 2, node)

        for edge in self.graph.edges:
            x1, y1 = self.graph.nodes[edge[0]]['pos']
            x2, y2 = self.graph.nodes[edge[1]]['pos']
            painter.setPen(Qt.blue)
            painter.drawLine(x1, y1, x2, y2)
            
    def resizeEvent(self, event):
        self.fitInView(self.sceneRect(), Qt.KeepAspectRatio)
        
def main():
    app = QApplication(sys.argv)
    
    graph = nx.DiGraph()
    graph.add_nodes_from(['a', 'b', 'c'])
    graph.add_edges_from([('a', 'b'), ('b', 'c')])
    pos = nx.spring_layout(graph)
    nx.set_node_attributes(graph, pos, 'pos')
    
    viewer = GraphViewer(graph)
    viewer.show()
    
    sys.exit(app.exec())

if __name__ == '__main__':
    main()