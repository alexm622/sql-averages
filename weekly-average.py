import mysql.connector

def get_averages(db: mysql.connector.MySQLConnection, appid: int):
    
    # get cursor
    cursor = db.cursor()

    # run command to get all numbers
    
    cursor.execute("SELECT numplayers FROM weekly_table WHERE appid = %s;", appid)
    
    # get results
    result = cursor.fetchall();
    
    # parse results into a list
    listResult = list(result)

    #get average
    count = len(listResult)
    avg = 0.0
    for x in listResult:
        y = list(x)
        y = float(y[0])
        avg = int(avg) + y
    avg = avg / float(count)

    return avg

db = mysql.connector.connect(
    host="10.0.0.6",
    user="script",
    passwd="script",
    database="gameserver"
)

cursor = db.cursor()

cursor.execute("SELECT DISTINCT appid FROM weekly_table;")

myresult = cursor.fetchall()

listResult = list(myresult)

avgs = []
appids = []
for x in listResult:
    appids.append(x)
    avgs.append(get_averages(db, x))


temp = 0
for x in appids:
    avg = avgs[temp]
    avg = int(avg)
    y = list(x)
    y = int(y[0])
    print(str(y))
    print(str(avg))
    val = (y, avg)
    sql = "INSERT INTO monthly_table (appid, numplayers) VALUES (%s, %s)"
    cursor.execute(sql, val)
    db.commit()
    temp += 1

#cursor.execute("DELETE FROM weekly_table")
