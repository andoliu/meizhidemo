import pymysql
import pandas as pd


db = pymysql.connect(host='lab-ms-d1-ex.mysql.rds.aliyuncs.com', port=3306, user='temptest', passwd='1234qwer', db='drtestdb')
cur = db.cursor()

#  todo 获取跨集团管理系统中数据库链接地址、用户名和密码以及对应的存储字段
host = []
user = []
password = []
port = []

# 此SQL主要是通过匹配吃数据库中的找出对应的数据库表，从而找到对应的数据库连接表中需要的字段
sql = "SELECT database_connectivity.connectivity_url, database_connectivity.connectivity_user, database_connectivity.connectivity_password, database_connectivity.connectivity_script FROM database_connectivity, `database`, sink_database WHERE database_connectivity.database_management_system_id = `database`.database_management_system_id AND `database`.database_id = sink_database.sink_database_key"
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


# todo 获取数据要存储的数据库以及要存储的字段
# 此SQL是通过匹配池数据库的池数据库key和数据库中的数据库ID，得到数据库技术名
sql1 = "SELECT `database`.database_tec_name FROM `database`, sink_database WHERE `database`.database_id = sink_database.sink_database_key"
cur.execute(sql1)
order_database = cur.fetchall()

sql2 = "SELECT database_table.table_tec_name FROM database_table, `database`, sink_database WHERE database_table.database_id = `database`.database_id AND `database`.database_id = sink_database.sink_database_key"
cur.execute(sql2)
order_table = cur.fetchall()


cur.close()
db.close()


# todo 读取数据库并储存需要数据
db1 = pymysql.connect(host='lab-ms-d1-ex.mysql.rds.aliyuncs.com', port=3306, user='temptest', passwd='1234qwer', db=order_database[0][0])
cur1 = db1.cursor()
for i in range(len(database)):
    print(type(database.loc[i, 'port']))
    newdb = pymysql.connect(host=database.loc[i, 'host'], port=int(database.loc[i, 'port']), user=database.loc[i, 'user'], passwd=database.loc[i, 'password'])
    newcur = newdb.cursor()
    newcur.execute('SHOW DATABASES')
    print(newcur.fetchall())
    for x in order_table:
        print(x)
        # sql3 = "CREATE TABLE %s" %(x[0])
        x1 = x[0].replace('-', '')
        x1 = x1.replace('_', '')
        print(x1)
        sql4 = "INSERT INTO db1.%s SELECT * FROM newdb.%s" % (x1, x1)
        cur1.execute(sql4)
        # newsql = "SELECT * FROM %s" % x
        # # newcur.execute(newsql)
        # a = pd.read_sql(newsql, newdb)
        # col_name_list = [tuple[0] for tuple in newcur.description]
        # data = newcur.fetchall()
        # create_sql = "CREATE TABLE admin(id CHAR )"
        # cur1.execute(create_sql)
        # insert_sql = "INSERT INTO admin_payinfo() VALUE()"
        # cur1.executemany(insert_sql, data)



