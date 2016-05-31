
#coding:gbk
import MySQLdb
import time
import datetime
mysqlHost = "localhost"
mysqlDB="didi"
mongoHost="localhost"
user1="root"
password1='root'
conn=MySQLdb.connect(host='localhost',user=user1,passwd=password1,db=mysqlDB,port=3306)
cur=conn.cursor()
cur.execute('create table order1(orderID varchar(300),driverID varchar(300),passengerID varchar(300),start_district_hash varchar(300),dest_district_hash varchar(300),price double,time1 time)')

f = open("D:\\data science\\citydata\\season_1\\training_data\\order_data\\order_data_2016-01-01","r")
lines = f.readlines()

for line in lines:
    conn=MySQLdb.connect(host='localhost',user=user1,passwd=password1,db=mysqlDB,port=3306)
    cur=conn.cursor()
    values=[]
    order = line.split("\t")
    orderID =order[0]
    driverID = order[1]
    passengerID = order[2]
    start_district_hash=order[3]
    dest_district_hash=order[4]
    price = order[5]
    time1 = order[6].split("\n")[0]
    # time2 = datetime.datetime(int(time1))
    createAt= datetime.datetime.strptime(time1,'%Y-%m-%d %H:%M:%S')
    values.append((orderID.encode("utf-8"),driverID.encode("utf-8"),passengerID.encode("utf-8"),start_district_hash.encode("utf-8"),dest_district_hash.encode("utf-8"),price,createAt))
    cur.executemany('insert into order1 values(%s,%s,%s,%s,%s,%s,%s)',values)
    conn.commit()
    cur.close()
    conn.close()
