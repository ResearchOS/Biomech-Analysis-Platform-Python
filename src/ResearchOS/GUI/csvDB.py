from initLoginSettings import getLoginInfoPath

def loadDB(csvPath=getLoginInfoPath()):    
    import csv
    import os    

    if os.path.exists(csvPath) == False:
        with open(csvPath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Username', 'Password', 'Email'])
            
    with open(csvPath, 'r') as f:
        reader = csv.reader(f)
        db = list(reader)
    return db

def getUserInfo(rowVal="", colName="Username", dbPath=getLoginInfoPath()):
    # If rowVal is empty, return all users. If not, return just one user
    colNames = ["Username", "Password", "Email"]
    colNum = colNames.index(colName)        

    db = loadDB(dbPath)

    if len(db) <= 1:
        return []
        
    allUsers = []    
    with open(dbPath, 'r') as f:
        for row in db[1:]:
            allUsers.append(row[colNum])

    # Get just one user 
    if rowVal:
        idx = [i for i, x in enumerate(allUsers) if x == rowVal]
        allUsers = allUsers[idx]

    return allUsers

def addToDatabase(self, username, password, email):
    import csv    

    with open('loginInfo.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([username, password, email])
    return