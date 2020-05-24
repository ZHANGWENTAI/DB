import pyodbc as odbc
from CreateTable import CreateTable

# 初始化数据库，返回连接句柄
def initDB():
    try:
        connection = odbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=课设')
    except odbc.DatabaseError as err:
        print(err)
    else:
        return connection

# 打印查询结果
def printTable(c, tableName):
    print("-----------------------")
    script = """SELECT * FROM {0}""".format(tableName)
    try:
        c.execute(script)
    except odbc.DatabaseError as err:
        print(err)
    row = c.fetchone()
    while row:
        print(row)
        row = cursor.fetchone()
    print("-----------------------")

# 主程序
if __name__ == '__main__':
    cnxn = initDB() # 初始化数据库连接
    cursor = cnxn.cursor() # 建立游标
    cursor.fast_executemany = True
    # createTable = CreateTable() #初始化数据表
    # createTable.createTable(cursor) # 创建表
    # createTable.loadData(cursor) #加载数据

    cursor.close()
    cnxn.close()