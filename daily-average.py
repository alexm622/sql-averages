import mysql.connector

def get_averages(db: mysql.connector.MySQLConnection, appid: int):
    
    # get cursor
    cursor = db.cursor()

    # run command to get all numbers
    
    cursor.execute("SELECT numplayers FROM daily_table WHERE appid = %s;", appid)
    
    # get results
    result = cursor.fetchall();
    
    # parse results into a list
    listResult = list(result)

    #get average
    count = len(listResult)
    avg = 0.0
    maximum = max(listResult)
    for x in listResult:
        y = list(x)
        y = float(y[0])
        avg = float(avg) + y
    avg = avg / float(count)

    return avg, maximum

db = mysql.connector.connect(
    host="10.0.0.6",
    user="script",
    passwd="script",
    database="gameserver"
)

cursor = db.cursor()

cursor.execute("SELECT DISTINCT appid FROM daily_table;")

myresult = cursor.fetchall()

listResult = list(myresult)

avgs = []
appids = []
maximums = []
for x in listResult:
    appids.append(x)
    avg, maximum = get_averages(db, x)
    avgs.append(avg)
    maximums.append(maximum)


temp = 0
for x in appids:
    avg = avgs[temp]
    avg = float(avg)
    y = list(x)
    y = int(y[0])
    print(str(y))
    print(str(avg))
    val = (y, avg)
    sql = "INSERT INTO weekly_table (appid, numplayers) VALUES (%s, %s)"
    cursor.execute(sql, val)

    maximum = maximums[temp]
    maximum = list(maximum)
    maximum = int(maximum[0])

    val = (y, maximum)
    sql = "INSERT INTO weekly_maxes (appid, max) VALUES (%s, %s)"
    cursor.execute(sql, val)
    db.commit()
    temp += 1

cursor.execute("DELETE FROM daily_table")
