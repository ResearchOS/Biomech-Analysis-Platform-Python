from PySide6 import QtWidgets, QtCore

def createRectangle(self):
    # 1. Create the rectangle itself

    # 2. Decide where to place it. Inside of the scrollArea (width: 741, height: 80)
    # Each rectangle is width: 70, height: 80
    # First rectangle position: (0, 0, 70, 80)
    # Second rectangle position: (70, 0, 70, 80)
    # Need to know: How many rectangles are already in the scrollArea?
    width = 70
    height = 80    
    rectCount = self.rectCount
    scrollArea = self.scrollArea
    scrollAreaWidgetContents = self.scrollAreaWidgetContents

    self.pushButton2 = QtWidgets.QPushButton(self.scrollAreaWidgetContents)
    self.pushButton2.setGeometry(QtCore.QRect(rectCount*width, 0, width, height))
    self.pushButton2.setObjectName("pushButton2")

    self.rectCount += 1