import pymysql

def insert_update_delete(qry,val):
    con=pymysql.connect(
        host='localhost',
        user='root',
        port=3306,
        password="7592978136",
        db='prod_live'

    )
    cmd=con.cursor()
    cmd.execute(qry,val)
    id=cmd.lastrowid
    con.commit()
    con.close()
    return  id

def insert_update_delete_many(qry,val):
    con=pymysql.connect(
        host='localhost',
        user='root',
        port=3306,
        password="7592978136",
        db='prod_live'

    )
    cmd=con.cursor()
    cmd.executemany(qry,val)
    id=cmd.lastrowid
    con.commit()
    con.close()
    return  id

def select_all(qry):
    con=pymysql.connect(
        host='localhost',
        user='root',
        port=3306,
        password="7592978136",
        db='prod_live'

    )
    cmd=con.cursor()
    cmd.execute(qry)
    res=cmd.fetchall()
    con.commit()
    con.close()
    return  res


def select_filter(qry,val):
    con=pymysql.connect(
        host='localhost',
        user='root',
        port=3306,
        password="7592978136",
        db='prod_live'

    )
    cmd=con.cursor()
    cmd.execute(qry,val)
    res=cmd.fetchall()
    con.commit()
    con.close()
    return  res



def select_one(qry,val):
    con=pymysql.connect(
        host='localhost',
        user='root',
        port=3306,
        password="7592978136",
        db='prod_live'

    )
    cmd=con.cursor()
    cmd.execute(qry,val)
    res=cmd.fetchone()
    con.commit()
    con.close()
    return  res