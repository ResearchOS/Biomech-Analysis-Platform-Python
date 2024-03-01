import os, sys

def get_computer_id() -> str:
    "Method from: https://gist.github.com/angeloped/3febaaf71ac083bc2cd5d99d775921d0"
    os_type = sys.platform.lower()
    if "win" == os_type:
        command = "wmic bios get serialnumber"
    elif "linux" == os_type:
        command = "hal-get-property --udi /org/freedesktop/Hal/devices/computer --key system.hardware.uuid"
    elif "darwin" == os_type:
        command = "ioreg -l | grep IOPlatformSerialNumber"
        
    string = os.popen(command).read().replace("\n","").replace("    ","").replace(" ","")
    
    # My modification
    if "win" == os_type:
        pass
    elif "linux" == os_type:
        pass
    elif "darwin" == os_type:        
        string = string.replace("IOPlatformSerialNumber","").replace("=","").replace("<","").replace(">","").replace('|',"").replace(" ","").replace('"',"")
    else:
        raise ValueError("Unsupported system platform.")
        
    return string

a = get_computer_id()
COMPUTER_ID = a # To ensure the function won't be re-run.
