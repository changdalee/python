import sqlite3

conn = sqlite3.connect('akshare.db')
print ("数据库打开成功")
c = conn.cursor()