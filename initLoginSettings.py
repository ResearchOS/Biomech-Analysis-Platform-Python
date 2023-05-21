import os
from initAllSettings import getSettingsPath

# Create the loginInfo.csv file if it does not exist
def initLoginInfo():
    settingsPath = getSettingsPath()
    csvPath = os.path.join(settingsPath, 'loginInfo.csv')
    if not os.path.exists(settingsPath):
        os.mkdir(settingsPath)
        
    if not os.path.exists(csvPath):
        with open(csvPath, 'w') as f:
            f.write('Username,Password,Email\n')

# Get the path to the login settings file
def getLoginInfoPath():
    settingsPath = getSettingsPath()
    csvPath = os.path.join(settingsPath, 'loginInfo.csv')
    return csvPath