from PySide6.QtWidgets import QApplication, QMainWindow
from login import LoginDialog
from createWidgets import createWidgets

import sys

# Subclass QMainWindow to customise the application's main window
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()  # Initialise the superclass

        # Create all of the widgets
        self.createWidgetsInit() # Layout should have the handles to all the GUI objects

    def createWidgetsInit(self):
        createWidgets(self)        

app = QApplication(sys.argv)

window = MainWindow()

loginWindow = LoginDialog(window)
loginWindow.show()

app.exec()