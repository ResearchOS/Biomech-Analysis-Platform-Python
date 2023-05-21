from defLoginSignals import handleLogin, forgotPassword, handleSignup

def setLoginWindowSignals(ui):
    ui.signinButton.clicked.connect(lambda: handleLogin(ui.usernameEdit.text(), ui.passwordEdit.text()))

    ui.forgotPassButton.clicked.connect(forgotPassword)

    ui.signupButton.clicked.connect(handleSignup)