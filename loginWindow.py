# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Python_Playground/PyGUI_PySide6/test_login.ui'
#
# Created by: PyQt5 UI code generator 5.9.2
#
# WARNING! All changes made in this file will be lost!

from PySide6 import QtCore, QtGui, QtWidgets

class Ui_loginWindow(object):
    def setupUi(self, loginWindow):
        loginWindow.setObjectName("loginWindow")
        loginWindow.setEnabled(True)
        loginWindow.resize(260, 382)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(loginWindow.sizePolicy().hasHeightForWidth())
        loginWindow.setSizePolicy(sizePolicy)
        loginWindow.setMinimumSize(QtCore.QSize(0, 0))
        self.verticalLayout = QtWidgets.QVBoxLayout(loginWindow)
        self.verticalLayout.setObjectName("verticalLayout")
        self.usernameFrame = QtWidgets.QFrame(loginWindow)
        self.usernameFrame.setMaximumSize(QtCore.QSize(16777215, 50))
        self.usernameFrame.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.usernameFrame.setFrameShadow(QtWidgets.QFrame.Plain)
        self.usernameFrame.setObjectName("usernameFrame")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.usernameFrame)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.usernameLabel = QtWidgets.QLabel(self.usernameFrame)
        self.usernameLabel.setMaximumSize(QtCore.QSize(16777215, 20))
        self.usernameLabel.setObjectName("usernameLabel")
        self.verticalLayout_2.addWidget(self.usernameLabel)
        self.usernameEdit = QtWidgets.QLineEdit(self.usernameFrame)
        self.usernameEdit.setMinimumSize(QtCore.QSize(0, 20))
        self.usernameEdit.setMaximumSize(QtCore.QSize(16777215, 20))
        self.usernameEdit.setText("")
        self.usernameEdit.setObjectName("usernameEdit")
        self.verticalLayout_2.addWidget(self.usernameEdit)
        self.verticalLayout.addWidget(self.usernameFrame, 0, QtCore.Qt.AlignHCenter)
        self.passwordFrame = QtWidgets.QFrame(loginWindow)
        self.passwordFrame.setMaximumSize(QtCore.QSize(16777215, 50))
        self.passwordFrame.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.passwordFrame.setFrameShadow(QtWidgets.QFrame.Raised)
        self.passwordFrame.setObjectName("passwordFrame")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.passwordFrame)
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.passwordLabel = QtWidgets.QLabel(self.passwordFrame)
        self.passwordLabel.setObjectName("passwordLabel")
        self.verticalLayout_3.addWidget(self.passwordLabel)
        self.passwordEdit = QtWidgets.QLineEdit(self.passwordFrame)
        self.passwordEdit.setMinimumSize(QtCore.QSize(0, 20))
        self.passwordEdit.setText("")
        self.passwordEdit.setObjectName("passwordEdit")
        self.verticalLayout_3.addWidget(self.passwordEdit)
        self.verticalLayout.addWidget(self.passwordFrame, 0, QtCore.Qt.AlignHCenter)
        self.signinFrame = QtWidgets.QFrame(loginWindow)
        self.signinFrame.setMaximumSize(QtCore.QSize(16777215, 80))
        self.signinFrame.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.signinFrame.setFrameShadow(QtWidgets.QFrame.Raised)
        self.signinFrame.setObjectName("signinFrame")
        self.verticalLayout_4 = QtWidgets.QVBoxLayout(self.signinFrame)
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.signinButton = QtWidgets.QPushButton(self.signinFrame)
        self.signinButton.setDefault(True)
        self.signinButton.setObjectName("signinButton")
        self.verticalLayout_4.addWidget(self.signinButton)
        self.forgotPassButton = QtWidgets.QPushButton(self.signinFrame)
        self.forgotPassButton.setObjectName("forgotPassButton")
        self.verticalLayout_4.addWidget(self.forgotPassButton)
        self.verticalLayout.addWidget(self.signinFrame, 0, QtCore.Qt.AlignHCenter)
        spacerItem = QtWidgets.QSpacerItem(20, 20, QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Maximum)
        self.verticalLayout.addItem(spacerItem)
        self.signupFrame = QtWidgets.QFrame(loginWindow)
        self.signupFrame.setMaximumSize(QtCore.QSize(16777215, 120))
        self.signupFrame.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.signupFrame.setFrameShadow(QtWidgets.QFrame.Raised)
        self.signupFrame.setObjectName("signupFrame")
        self.verticalLayout_5 = QtWidgets.QVBoxLayout(self.signupFrame)
        self.verticalLayout_5.setObjectName("verticalLayout_5")
        self.label = QtWidgets.QLabel(self.signupFrame)
        self.label.setObjectName("label")
        self.verticalLayout_5.addWidget(self.label, 0, QtCore.Qt.AlignHCenter)
        self.signupLabel = QtWidgets.QLabel(self.signupFrame)
        self.signupLabel.setMaximumSize(QtCore.QSize(16777215, 20))
        self.signupLabel.setObjectName("signupLabel")
        self.verticalLayout_5.addWidget(self.signupLabel, 0, QtCore.Qt.AlignHCenter)
        self.signupButton = QtWidgets.QPushButton(self.signupFrame)
        self.signupButton.setMaximumSize(QtCore.QSize(100, 30))
        self.signupButton.setObjectName("signupButton")
        self.verticalLayout_5.addWidget(self.signupButton, 0, QtCore.Qt.AlignHCenter)
        self.verticalLayout.addWidget(self.signupFrame, 0, QtCore.Qt.AlignHCenter)

        self.retranslateUi(loginWindow)
        QtCore.QMetaObject.connectSlotsByName(loginWindow)

    def retranslateUi(self, loginWindow):
        _translate = QtCore.QCoreApplication.translate
        loginWindow.setWindowTitle(_translate("loginWindow", "Login"))
        self.usernameLabel.setText(_translate("loginWindow", "Username"))
        self.passwordLabel.setText(_translate("loginWindow", "Password"))
        self.signinButton.setText(_translate("loginWindow", "Sign In"))
        self.forgotPassButton.setText(_translate("loginWindow", "Forgot Password"))
        self.label.setText(_translate("loginWindow", "First time?"))
        self.signupLabel.setText(_translate("loginWindow", "Sign up using the button below"))
        self.signupButton.setText(_translate("loginWindow", "Sign Up"))
