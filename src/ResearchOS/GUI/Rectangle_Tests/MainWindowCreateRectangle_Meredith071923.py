from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QFrame, QScrollArea
from PySide6.QtCore import Qt

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.count = 0
        self.countt = 0
        
        self.setWindowTitle("Data Analysis")

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        self.button = QPushButton("Create New Function")
        self.button.setFixedSize(150, 40)
        self.button.setDisabled(True)  # Initially disable the button
        self.button.clicked.connect(self.add_rectangle)

        self.button2 = QPushButton("Add New Row")
        self.button2.setFixedSize(150, 40)
        self.button2.clicked.connect(self.add_row)

        self.scroll_layout = QVBoxLayout()
        self.scroll_widget = QWidget()
        self.scroll_widget.setLayout(self.scroll_layout)

        self.frame = None  # Initially no row is added

        self.scroll_area.setWidget(self.scroll_widget)

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.button2)
        self.layout.addWidget(self.button)
        self.layout.addWidget(self.scroll_area)

        self.central_widget = QWidget()
        self.central_widget.setLayout(self.layout)
        self.setCentralWidget(self.central_widget)

        self.add_row()  # Initially add a row

    def add_rectangle(self):
        self.count += 1

        rect_widget = QPushButton()
        rect_widget.setFixedSize(100, 50)
        rect_widget.setCheckable(True)
        rect_widget.setStyleSheet("QWidget {background-color: #c1e1ec;}"
                                  "QWidget:hover {background-color: #99cfe0;}"
                                  "QWidget {border: 1px solid white;}"
                                  "QWidget::checked {background-color:  #5fb4ce;}")
        rect_widget.setObjectName("rectangle_widget")

        self.frame_layout.addWidget(rect_widget)
    
    def add_row(self):
        self.countt += 1

        self.frame = QFrame(self.scroll_widget)
        self.frame_layout = QHBoxLayout(self.frame)
        self.scroll_layout.addWidget(self.frame)

        self.button.setEnabled(True)  # Enable the "Create New Function" button


if __name__ == "__main__":
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()

