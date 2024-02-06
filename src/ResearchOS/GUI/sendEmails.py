from PySide6.QtWidgets import QMessageBox

def sendEmail(sender_email, app_password, receiver_email, message):
    import smtplib
    
    try:
        # creates SMTP session
        s = smtplib.SMTP('smtp.gmail.com', 587)

        # start TLS for security
        s.starttls()

        # Authentication
        s.login(sender_email, app_password)    

        # sending the mail
        s.sendmail(sender_email, receiver_email, message)

        # terminating the session
        s.quit()

    except:
        QMessageBox.critical(None, "Error", "Error sending email. Check your email address and internet connection and try again.")