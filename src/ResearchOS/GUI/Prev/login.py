import time
import sys

from PySide6 import QtCore, QtSql
from PySide6.QtGui import QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QMainWindow, QDialog, QGridLayout, 
    QWidget, QStackedWidget, QLabel, QSizePolicy,
    QTabWidget, QPushButton, QLineEdit, QVBoxLayout, QHBoxLayout, QMessageBox
)

#from sqlTest import connectSQL, getUsers, addToDatabase
from csvDB import loadDB, getUsers, addToDatabase
from sendEmails import sendEmail

# Can either import from sqlTest to use SQL database, OR from another package to use CSV files

class LoginDialog(QMainWindow, QDialog):
    def __init__(self, mainWindow):
        super().__init__()
        self.initUI(mainWindow)
 
    def initUI(self,mainWindow):
        self.setWindowTitle('Login')
        self.setFixedSize(800, 500)

        self.createLoginWidgets(mainWindow)        

        if 'connectSQL' in sys.modules:
            self.myDB = connectSQL() # Connect to the mySQL database  
        # else:
            # self.myDB = loadDB() # Load the database from CSV file

    def handleLogin(self):
        username = self.signin_widgets["userEdit"].text()
        password = self.signin_widgets["passEdit"].text()

        if len(username) == 0 or len(password) == 0:
            QMessageBox.warning(
                self, 'Error', 'Please fill in all fields')
            return

        # myresult is the entire row of the database that matches the username
        myresult = getUsers(username, self.myDB)

        if len(myresult)>0:
            if myresult[0][2] == password:
                time.sleep(1)
                self.mainWindow.show()
                self.mainWindow.username = username
                self.close()
            else:   
                QMessageBox.warning(
                self, 'Error', 'Incorrect password')
        else:
            QMessageBox.warning(
                self, 'Error', 'Incorrect username')                      

    def handleSignup(self):
        username = self.signup_widgets["userEdit"].text()
        password = self.signup_widgets["passEdit"].text()        
        passwordConfirm = self.signup_widgets["passEditConfirm"].text()
        email = self.signup_widgets["emailEdit"].text()

        if len(username) == 0 or len(password) == 0 or len(passwordConfirm) == 0:
            QMessageBox.warning(
                self, 'Error', 'Please fill in all fields')
            return

        myresult = getUsers(self, username)

        if len(myresult)>0:
            QMessageBox.warning(
                self, 'Error', 'Username already exists')
            return

        # Create account if passwords match
        if password == passwordConfirm:
            # Add the user to the database
            addToDatabase(self, username, password, email)            
            QMessageBox.information(
                self, 'Success', 'Account created, please log in')
        
        # Tell user if passwords do not match
        else:
            QMessageBox.warning(
                self, 'Error', 'Passwords do not match') 

    def handleForgotPassword(self):
        username = self.signin_widgets["userEdit"].text()

        if len(username) == 0:
            QMessageBox.warning(
                self, 'Error', 'Please fill in the username field')
            return

        # check if username already exists
        myresult = getUsers(self, username)

        if len(myresult)==0:
            QMessageBox.warning(
                self, 'Error', 'Username does not exist')
            return

        if len(myresult)==1:
            receiver_email = myresult[0][3] # Get the email address associated with the account
            sender_email = "mtillman14@gmail.com"
            app_password = "fknrdequrznldctw"

            username = myresult[0][1]
            password = myresult[0][2]
            message = "BiomechOS login information\n\nUsername: " + username + "\nPassword: " + password

            sendEmail(sender_email, app_password, receiver_email, message)
        else:   
            QMessageBox.warning(
                self, 'Error', 'More than one user! How did this happen?')
            return

    def createLoginWidgets(self, mainWindow):
        # Create the widgets
        tabs = QTabWidget()
        self.setCentralWidget(tabs)
        tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        tabs.setTabPosition(QTabWidget.North)
        tabs.setMovable(False)

        # Sign-in tab
        signin_tab = QWidget()
        signup_tab = QWidget()

        signin_tab.layout = QGridLayout()
        signup_tab.layout = QGridLayout()

        # print(signin_widgets)     
        # Sign-in tab widgets
        signin_widgets={}        
        signin_widgets["userLabel"] = QLabel(self, text='Username:')     
        signin_widgets["passLabel"] = QLabel(self, text='Password:')      
        signin_widgets["userEdit"] = QLineEdit(self, placeholderText='Username')
        signin_widgets["passEdit"] = QLineEdit(self, echoMode=QLineEdit.Password, placeholderText='Password')        
        signin_widgets["loginButton"] = QPushButton(self, text='Login', clicked=self.handleLogin)
        signin_widgets["cancelButton"] = QPushButton(self, text='Cancel', clicked=self.close)
        signin_widgets["loginButton"].setDefault(True)
        signin_widgets["forgotPassButton"] = QPushButton(self, text='Forgot password?', clicked=self.handleForgotPassword)
        self.signin_widgets=signin_widgets

        for widget in signin_widgets.values():
            signin_tab.layout.addWidget(widget)

        # Sign-up tab widgets
        signup_widgets={}
        signup_widgets["emailLabel"] = QLabel(self, text='Email:')
        signup_widgets["emailEdit"] = QLineEdit(self, placeholderText='Email')
        signup_widgets["userLabel"] = QLabel(self, text='Username:')        
        signup_widgets["passLabel"] = QLabel(self, text='Password:')        
        signup_widgets["userEdit"] = QLineEdit(self, placeholderText='Username')
        signup_widgets["passEdit"] = QLineEdit(self, echoMode=QLineEdit.Password, placeholderText='Password')
        signup_widgets["passEditConfirm"] = QLineEdit(self, echoMode=QLineEdit.Password, placeholderText='Confirm password')
        signup_widgets["signupButton"] = QPushButton(self, text='Sign up', clicked=self.handleSignup)
        signup_widgets["cancelButton"] = QPushButton(self, text='Cancel', clicked=self.close)
        signup_widgets["signupButton"].setDefault(True)        
        self.signup_widgets=signup_widgets   

        for widget in signup_widgets.values():
            signup_tab.layout.addWidget(widget)     

        # Position the widgets
        layoutV={}
        layoutH={}
        layoutV["sign_in"]=QVBoxLayout(signin_tab)
        layoutH["sign_in"]=QHBoxLayout()      
        layoutV["sign_in"].setGeometry(QtCore.QRect(0, 0, self.width(), self.height()))   
        layoutV["sign_in"].addWidget(signin_widgets["userLabel"])
        layoutV["sign_in"].addWidget(signin_widgets["userEdit"])
        layoutV["sign_in"].addWidget(signin_widgets["passLabel"])
        layoutV["sign_in"].addWidget(signin_widgets["passEdit"])
        layoutV["sign_in"].addLayout(layoutH["sign_in"])
        layoutH["sign_in"].addWidget(signin_widgets["loginButton"])
        layoutH["sign_in"].addWidget(signin_widgets["cancelButton"])
        layoutV["sign_in"].addWidget(signin_widgets["forgotPassButton"])

        layoutV["sign_up"]=QVBoxLayout(signup_tab)
        layoutH["sign_up"]=QHBoxLayout()
        layoutV["sign_up"].setGeometry(QtCore.QRect(0, 0, self.width(), self.height()))
        layoutV["sign_up"].addWidget(signup_widgets["emailLabel"])
        layoutV["sign_up"].addWidget(signup_widgets["emailEdit"])
        layoutV["sign_up"].addWidget(signup_widgets["userLabel"])
        layoutV["sign_up"].addWidget(signup_widgets["userEdit"])
        layoutV["sign_up"].addWidget(signup_widgets["passLabel"])
        layoutV["sign_up"].addWidget(signup_widgets["passEdit"])
        layoutV["sign_up"].addWidget(signup_widgets["passEditConfirm"])
        layoutV["sign_up"].addLayout(layoutH["sign_up"])
        layoutH["sign_up"].addWidget(signup_widgets["signupButton"])
        layoutH["sign_up"].addWidget(signup_widgets["cancelButton"])

        signin_tab.setLayout(signin_tab.layout)
        signup_tab.setLayout(signup_tab.layout)

        self.mainWindow = mainWindow

        tabs.insertTab(0, signin_tab, 'Sign in')
        tabs.insertTab(1, signup_tab, 'Sign up')

        # Create shortcuts
        self.enterShortcut = QShortcut(QKeySequence("Return"), signin_tab)
        self.enterShortcut.activated.connect(self.handleLogin)

        self.enterShortcut = QShortcut(QKeySequence("Return"), signup_tab)
        self.enterShortcut.activated.connect(self.handleSignup)