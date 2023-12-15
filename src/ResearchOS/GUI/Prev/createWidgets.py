from PySide6.QtWidgets import (
    QTabWidget, QPushButton, 
    QGridLayout, QWidget, 
    QStackedWidget, QLabel, 
    QSizePolicy
)

def createWidgets(window):

    window.setWindowTitle("Biomech OS")

    # Create the widgets    
    tabs = QTabWidget()    
    window.setCentralWidget(tabs)
    tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
    tabs.setTabPosition(QTabWidget.West)
    tabs.setMovable(False)
    
    projectTab = QWidget()
    logsheetTab = QWidget()
    processTab = QWidget()
    plotTab = QWidget()
    statsTab = QWidget()
    settingsTab = QWidget()    

    projectTab.layout = QGridLayout()
    logsheetTab.layout = QGridLayout()
    processTab.layout = QGridLayout()
    plotTab.layout = QGridLayout()
    statsTab.layout = QGridLayout()
    settingsTab.layout = QGridLayout()

    # Create the project tab widgets
    # createProjectTabWidgets(projectTab)
    projectLabel = projectTab.layout.addWidget(QLabel("Project Tab"))
    projectLabel2 = projectTab.layout.addWidget(QLabel("Project Tab Label 2"))

    # Create the logsheet tab widgets
    logsheetLabel = logsheetTab.layout.addWidget(QLabel("Logsheet Tab"))

    # Create the process tab widgets

    # Create the plot tab widgets

    # Create the stats tab widgets

    # Create the settings tab widgets
    
    # Set the layout of each tab
    projectTab.setLayout(projectTab.layout)
    logsheetTab.setLayout(logsheetTab.layout)
    processTab.setLayout(processTab.layout)
    plotTab.setLayout(plotTab.layout)
    statsTab.setLayout(statsTab.layout)
    settingsTab.setLayout(settingsTab.layout)
    
    # Add tabs to the tab widget
    tabs.insertTab(0, projectTab, "Projects")
    tabs.insertTab(1, logsheetTab, "Logsheet")
    tabs.insertTab(2, processTab, "Process")
    tabs.insertTab(3, plotTab, "Plot")
    tabs.insertTab(4, statsTab, "Stats")
    tabs.insertTab(5, settingsTab, "Settings")