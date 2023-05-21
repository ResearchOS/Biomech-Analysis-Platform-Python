# Define the methods for the login signals
import re

from PySide6.QtWidgets import QInputDialog, QMessageBox, QDialog, QDialogButtonBox

from sendEmails import sendEmail
from csvDB import loadDB, getUserInfo, addToDatabase
import credentials

def handleLogin(loginDlg):
    # 1. Get the username and password from the user
    username = loginDlg.username.text()

def forgotPassword():
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

    # 2. Send the user an email with the username & password associated with that email        
    sender_email = "mtillman14@gmail.com" # For now, use my email
    
    # Fix this later    
    users = getUserInfo(colName=receiver_email)
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