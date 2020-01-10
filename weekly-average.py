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
        avg = float(avg) + y
    avg = avg / float(count)

    cursor.execute("SELECT max FROM weekly_maxes WHERE appid = %s", appid)

    result = cursor.fetchall()

    listResult = list(result)
    
    maximum = max(listResult)


    return avg, maximum

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
maximums = []
appids = []
for x in listResult:
    appids.append(x)
    avg, maximim = get_averages(db, x)
    avgs.append(avg)
    maximums.append(maximim)


temp = 0
for x in appids:
    avg = avgs[temp]
    avg = float(avg)
    y = list(x)
    y = int(y[0])
    print(str(y))
    print(str(avg))
    val = (y, avg)
    sql = "INSERT INTO monthly_table (appid, numplayers) VALUES (%s, %s)"
    
    cursor.execute(sql, val)


    maximum = maximums[temp]
    maximum = list(maximum)
    maximum = int(maximum[0])

    val = (y, maximum)
    sql = "INSERT INTO monthly_maxes (appid, max) VALUES (%s, %s)"
    cursor.execute(sql, val)
    db.commit()
    temp += 1

cursor.execute("DELETE FROM weekly_table")
