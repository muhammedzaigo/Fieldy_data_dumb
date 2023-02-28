import pymysql

HOST = 'localhost'
USER = 'root'
PORT = 3306
PASSWORD = '7592978136'
DATABASE = 'prod_live'

connection = pymysql.connect(host=HOST,user=USER,port=PORT,password=PASSWORD,db=DATABASE)

def insert_update_delete(qry,val):
    con=connection
    cmd=con.cursor()
    cmd.execute(qry,val)
    id=cmd.lastrowid
    con.commit()
    con.close()
    return id


def insert_update_delete_many(qry,val):
    con=connection
    cmd=con.cursor()
    cmd.executemany(qry,val)
    rows=cmd.lastrowid    
    con.commit()
    con.close()
    return rows


def select_all(qry):
    con=connection
    cmd=con.cursor()
    cmd.execute(qry)
    res=cmd.fetchall()
    con.commit()
    con.close()
    return res


def select_filter(qry,val):
    con=connection
    cmd=con.cursor()
    cmd.execute(qry,val)
    res=cmd.fetchall()
    con.commit()
    con.close()
    return res


def select_one(qry,val):
    con=connection
    cmd=con.cursor()
    cmd.execute(qry,val)
    res=cmd.fetchone()
    con.commit()
    con.close()
    return res