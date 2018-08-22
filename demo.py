import pymysql
import pandas as pd


db = pymysql.connect(host='lab-ms-d1-ex.mysql.rds.aliyuncs.com', port=3306, user='temptest', passwd='1234qwer', db='drtestdb')
cur = db.cursor()

#  todo 获取跨集团管理系统中数据库链接地址、用户名和密码以及对应的存储字段
host = []
user = []
password = []
port = []

sql = "SELECT database_connectivity.connectivity_url, database_connectivity.connectivity_user, database_connectivity.connectivity_password, database_connectivity.connectivity_script FROM database_connectivity, database_management_system, client WHERE database_connectivity.database_management_system_id = database_management_system.database_management_system_id and database_management_system.logical_system_id = client.logical_system_id"
cur.execute(sql)
for r in cur.fetchall():
    print(r)
    print(type(r))
    host.append(r[0])
    user.append(r[1])
    password.append(r[2])
    port.append(r[3])


database = pd.DataFrame()
database['host'] = host
database['user'] = user
database['password'] = password
database['port'] = port


print(database)


# todo 获取数据要存储的数据池以及要存储的字段
sql1 = "SELECT database_connectivity.connectivity_url, database_connectivity.connectivity_user, database_connectivity.connectivity_password, database_connectivity.connectivity_script FROM database_connectivity, database_management_system, client WHERE database_connectivity.database_management_system_id = database_management_system.database_management_system_id and database_management_system.logical_system_id = client.logical_system_id"
cur.execute(sql)


# todo 读取外来数据库并储存需要数据
db1 = pymysql.connect(host='lab-ms-d1-ex.mysql.rds.aliyuncs.com', port=3306, user='temptest', passwd='1234qwer', db='datasink-2')
for i in range(len(database)):
    newdb = pymysql.connect(host=database[i, 'host'], port=database[i, 'port'], user=database[i, 'user'], passwd=database[i, 'password'])
    cur1 = newdb.cursor()
    newsql = "SELECT * FROM "
    cur1.execute(newsql)
    data = cur1.fetchall()
    create_sql = "CREATE TABLE admin(id CHAR )"
    cur.execute(create_sql)
    insert_sql = "INSERT INTO admin_payinfo() VALUE()"
    cur.executemany(insert_sql, data)



cur.close()
db.close()
