# This is the main file to run the application. This file opens the login window.
import sys, os

from PySide6 import QtWidgets

from loginWindow import Ui_loginWindow
from setLoginWindowSignals import setLoginWindowSignals
from initLoginSettings import initLoginInfo
# from initAllSettings import getSettingsPath

# Initialize settings files needed by the application.
# Currently uses the default settings path that most apps use.
# For cheap/easy cloud integration in the future, can use Google Drive File Stream and set the settings path to the Google Drive folder.
initLoginInfo()

## Create application using QtDesigner file
app = QtWidgets.QApplication(sys.argv)
loginWindow = QtWidgets.QWidget()
ui = Ui_loginWindow() # Instantiate (create an instance of) the Ui_loginWindow class
ui.setupUi(loginWindow)

# Do the UI setup that QtDesigner apparently can't do
loginWindow.setFixedSize(loginWindow.size()) # Because QtDesigner seems unable to do this in the GUI
ui.passwordEdit.setEchoMode(QtWidgets.QLineEdit.Password) # Because QtDesigner seems unable to do this in the GUI

# Signals and Slots (can potentially be done in QtDesigner, don't yet know how)
setLoginWindowSignals(ui)

# Show window
loginWindow.show()
sys.exit(app.exec())