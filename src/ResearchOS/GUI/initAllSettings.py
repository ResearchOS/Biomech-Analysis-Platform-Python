# Initialize all the settings files needed by the application, except for the login window.
import os

# Return the path to the settings folder
def getSettingsPath():
    username = os.getlogin()
    system = os.name
    if system == 'nt': # Windows
        settingsFolder = f'C:\\Users\\{username}\\AppData\\Local\\PGUI'
    elif system == 'posix': # Mac or Linux
        settingsFolder = f'/Users/{username}/Library/PGUI'

    return settingsFolder