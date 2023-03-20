import pymysql
import os

from dotenv import load_dotenv
load_dotenv()

HOST = str(os.getenv('HOST'))
USER = str(os.getenv('USER'))
PORT = os.getenv('PORT')
PASSWORD = str(os.getenv('PASSWORD'))
DATABASE = str(os.getenv('DATABASE'))

def insert_update_delete(qry,val):
    connection=pymysql.connect(host=HOST,user=USER,port=PORT,password=PASSWORD,db=DATABASE)
    cmd=connection.cursor()
    cmd.execute(qry,val)
    id=cmd.lastrowid
    connection.commit()
    connection.close()
    return id


def insert_update_delete_many(qry,val):
    connection=pymysql.connect(host=HOST,user=USER,port=PORT,password=PASSWORD,db=DATABASE)
    cmd=connection.cursor()
    cmd.executemany(qry,val)
    rows=cmd.lastrowid    
    connection.commit()
    connection.close()
    return 


def select_all(qry):
    connection=pymysql.connect(host=HOST,user=USER,port=PORT,password=PASSWORD,db=DATABASE)
    cmd=connection.cursor()
    cmd.execute(qry)
    res=cmd.fetchall()
    connection.commit()
    connection.close()
    return res


def select_filter(qry,val):
    connection=pymysql.connect(host=HOST,user=USER,port=PORT,password=PASSWORD,db=DATABASE)
    cmd=connection.cursor()
    cmd.execute(qry,val)
    res=cmd.fetchall()
    connection.commit()
    connection.close()
    return res


def select_one(qry,val):
    connection=pymysql.connect(host=HOST,user=USER,port=PORT,password=PASSWORD,db=DATABASE)
    cmd=connection.cursor()
    cmd.execute(qry,val)
    res=cmd.fetchone()
    connection.commit()
    connection.close()
    return res