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

        self.scroll_area.setWidget(self.scroll_widget)

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.button2)
        self.layout.addWidget(self.button)
        self.layout.addWidget(self.scroll_area)

        self.central_widget = QWidget()
        self.central_widget.setLayout(self.layout)
        self.setCentralWidget(self.central_widget)

        self.frame = None  # Initially no row is added
        self.current_button = None  # Initially no button is clicked

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
        rect_widget.clicked.connect(self.set_button_row_num)

        # If the there's no button clicked, set the row number to the current row.
        if not self.current_button:
            self.row_num = self.countt

        self.frame_layout[self.row_num].addWidget(rect_widget)

    def set_button_row_num(self):
        # Set the row number of the button that was clicked, and identify the button that was clicked.
        self.row_num = self.countt
        if self.sender() is not None:
            self.current_button = self.sender() # None if this method is called from add_row()
    
    def add_row(self):
        self.countt += 1

        self.frame[self.countt] = QFrame(self.scroll_widget)
        self.frame[self.count].frame_layout = QHBoxLayout(self.frame)
        self.scroll_layout[self.count].addWidget(self.frame)

        self.button.setEnabled(True)  # Enable the "Create New Function" button

        if not self.current_button:
            self.set_button_row_num()


if __name__ == "__main__":
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()

