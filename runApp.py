# This is the main file to run the application. This file opens the login window.
import sys, os

from PySide6 import QtWidgets
from PySide6.QtCore import QFile
from PySide6.QtUiTools import QUiLoader

from initLoginSettings import initLoginInfo
from defLoginSignals import handleLogin, forgotPassword, handleSignup

class LoginWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None, ui_filename=None, db_filename=None, next_window_filename=None):
        super().__init__()        
        self.parent = parent     
        self.next_window_filename = next_window_filename           
        
        ui_file = QFile(ui_filename)
        ui_file.open(QFile.ReadOnly)

        loader = QUiLoader()        
        loginWindow = loader.load(ui_file)   
        self.loginWindow = loginWindow 
        ui_file.close()    

        # Initialize settings files needed by the application.
        # Currently uses the default settings path that most apps use.
        # For cheap/easy cloud integration in the future, can use Google Drive File Stream and set the settings path to the Google Drive folder.
        initLoginInfo()
        self.setLoginWindowSignals(loginWindow, db_filename)                

        # # Do the UI setup that QtDesigner apparently can't do
        # loginWindow.setFixedSize(loginWindow.size()) # Because QtDesigner seems unable to do this in the GUI
        # loginWindow.passwordEdit.setEchoMode(QtWidgets.QLineEdit.Password) # Because QtDesigner seems unable to do this in the GUI         

        loginWindow.show()

    def setLoginWindowSignals(self, ui, db_filename=None):
        ui.signinButton.clicked.connect(lambda: handleLogin(self, ui.usernameEdit.text(), ui.passwordEdit.text(), db_filename))

        ui.forgotPassButton.clicked.connect(lambda: forgotPassword())

        ui.signupButton.clicked.connect(lambda: handleSignup())

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    ui_filename = 'loginWindow.ui'
    db_filename = 'loginInfo.csv'
    next_window_filename = 'mainWindow.py' # Code that executes the main window file.
    LoginWindow(app, ui_filename, db_filename, next_window_filename) # The app is the parent of the loginWindow    
    sys.exit(app.exec())