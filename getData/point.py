
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
cur.execute('create table point(district_hash varchar(60),pointlev1 int,pointlev2 int,point int)')

f = open("D:\\data science\\citydata\season_1\\training_data\poi_data\\poi_data","r")
lines = f.readlines()

for line in lines:
    conn=MySQLdb.connect(host='localhost',user=user1,passwd=password1,db=mysqlDB,port=3306)
    cur=conn.cursor()
    values=[]
    point = line.split("\t")
    for i in range(1,len(point)):
        district_hash=point[0]
        poi=point[i]
        lp1 = 0
        lp2 = 0
        lpo = 0
        a=poi.split("#")
        b = len(a)
        if("#" in poi and b==2):
            lp1 = poi.split("#")[0]
            lp2 =poi.split("#")[1].split(":")[0]
            lpo = poi.split("#")[1].split(":")[1]
        elif(b==1):
                lp2 =poi.split(":")[0]
                lpo = poi.split(":")[1]
        else:
            print poi
        values.append((district_hash.encode("utf-8"),lp1,lp2,lpo))
    cur.executemany('insert into point values(%s,%s,%s,%s)',values)
    conn.commit()
    cur.close()
    conn.close()
