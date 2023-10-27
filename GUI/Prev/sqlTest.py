def connectSQL():
    import mysql.connector

    try: # Access the database if it exists
        db_exist = True
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="spain1996",
            database="loginInfo"
        )
    except: # Access a "generic" database if it doesn't exist?
        db_exist = False
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="spain1996"
        )

    mycursor = mydb.cursor()
    
    # If the database doesn't exist, create it adn the table in it
    if not db_exist:
        mycursor.execute("CREATE DATABASE loginInfo")
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="spain1996",
            database="loginInfo"
        )
        mycursor = mydb.cursor()
        mycursor.execute("DROP TABLE loginInfo")
        mycursor.execute("CREATE TABLE loginInfo (id INT AUTO_INCREMENT PRIMARY KEY, Username VARCHAR(255), Password VARCHAR(255), Email VARCHAR(255))")

    return mydb

def getUsers(username, db=[]):
    mycursor = db.cursor()

    sql = "SELECT * FROM loginInfo WHERE Username = %s"
    val = (username,)

    mycursor.execute(sql, val)

    myresult = mycursor.fetchall()

    return myresult

def addToDatabase(username, password, email, db=[]):
    mycursor = db.cursor()
    sql = "INSERT INTO loginInfo (Username, Password, Email) VALUES (%s, %s, %s)"
    val = (username, password, email)
    mycursor.execute(sql, val)
    db.commit()