# Define the methods for the login signals
import re

from PySide6.QtWidgets import QInputDialog, QMessageBox, QDialog, QDialogButtonBox

from sendEmails import sendEmail
from csvDB import loadDB, getUserInfo, addToDatabase
import credentials

def handleLogin(self, username, password, db_filename=None):
    # 1. Check if the username exists
    users = getUserInfo(username, "Username", db_filename)

    # 2. Check if the password exists

    # 3. If both exist and are on the same row, then login

    # 4. If the username and password combination does not exist, tell the user to try again or sign up
    self.loginWindow.close()
    exec(self.next_window_filename)


def forgotPassword(loginInfoPath=None):
    # 1. Prompt the user for an email
    emailDlg = QInputDialog()
    emailDlg.setInputMode(QInputDialog.TextInput)
    emailDlg.setWindowTitle("Forgot Password")
    emailDlg.setLabelText("Enter your email address:")
    emailDlg.resize(500, 200)
    emailDlg.exec()

    # The user cancelled the dialog
    if not emailDlg.result():
        return
    
    receiver_email = emailDlg.textValue()
    
    # 2. Check if the email is a valid format
    pat = "^[a-zA-Z0-9-_]+@[a-zA-Z0-9]+\.[a-z]{1,3}$"
    if not re.match(pat, receiver_email):
        invalidEmailDlg = QMessageBox()
        invalidEmailDlg.setText("Invalid Email")        
        invalidEmailDlg.exec()       
        emailDlg.close()
        forgotPassword() # Restart the process
        return   

    # 2. Send the user an email with the username & password associated with that email        
    sender_email = "mtillman14@gmail.com" # For now, use my email
    
    ### Fix this later ###
    users = getUserInfo(colName=receiver_email, dbPath=loginInfoPath)
    idx = [i for i, x in enumerate(users) if x == receiver_email]
    username = users[idx]
    password = users[idx]
    message = "Username: " + username + "\nPassword: " + password    
    sendEmail(sender_email, credentials.app_password, receiver_email, message)

    # 3. Tell the user to check their email
    okDlg = QMessageBox()
    okDlg.setWindowTitle("Email Sent")
    okDlg.setText("Check your email for your username and password.")    
    okDlg.exec()
    emailDlg.close()    

def handleSignup():
    pass
    # 1. Open a new window to prompt the user for their username, password, and email

    # 2. Check if the username already exists

    # 3. If it does not exist, add the user to the database

    # 4. If it does exist, tell the user to pick a different username, or click the "Forgot Password" button

    # 5. If the user clicks the "Forgot Password" button, call the forgotPassword() method

    # 6. If the user clicks the "Cancel" button, close the window and reopen the login window