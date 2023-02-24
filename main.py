
import io
from flask import*
import os
import csv
app = Flask (__name__)

app.secret_key="f#6=zf!2=n@ed-=6g17&k4e4fl#d4v&l*v6q5_6=8jz1f98v#"

from database_connection import *

UPLOAD_FOLDER = "media"

@app.route("/api")
def api():
    import datetime
    start = datetime.datetime.now()
    qry = "SELECT * FROM `customer_group` LIMIT 1"
    result = select_all(qry)
    data = {"diffence" :  str(datetime.datetime.now() - start),"data": result}
    response = make_response(jsonify(data),200)
    response.headers["Content-Type"] = "application/json"
    return response

@app.route("/api/customer_group",methods=['POST'])
def customer_group():
    import datetime
    start = datetime.datetime.now()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})
    file = request.files['file']
    import_sheet = file.read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(import_sheet))
    customer_group = []
    address = []
    emails = []
    lines = []
    for line in reader:
        first_name = line.get('first_name')
        last_name = line.get('last_name')
        email = line.get('email')
        address1 = line.get('address1')
        address2 = line.get('address2')
        name = first_name+" "+last_name
        time = datetime.datetime.now()
        customer_group.append((name,email,15,time))
        address.append((15,address1,address2,time))
        emails.append(email)
        lines.append(address1)

    # ---------------------------------------------------------------- single Delete ------------------------------------------------------------------        
        
        # try:
        #     qry = "DELETE FROM `customer_group` WHERE  `email` = %s"
        #     insert_update_delete(qry, email)
        # except Exception as e:
        #     print(str(e))
            
            
    # ---------------------------------------------------------------- Bulk Delete ------------------------------------------------------------------        
    # try:
        # qry = "DELETE FROM `customer_group` WHERE  `email` IN (%s) "
        # insert_update_delete_many(qry, emails)
    #     qry = "DELETE FROM `addresses` WHERE  `line_1` IN (%s)"
    #     insert_update_delete_many(qry, lines)
    # except Exception as e:
    #     print(str(e))
        
    # ---------------------------------------------------------------- Bulk Insert ------------------------------------------------------------------        
        
    try:
        qry = "INSERT INTO `customer_group`(`name`,`email`,`tenant_id`,`created_at`) VALUES (%s,%s,%s,%s)"  
        insert_update_delete_many(qry,customer_group)
    except Exception as e:
        print(f"customer_group : {str(e)}")
        
    try:
        qry = "INSERT INTO `addresses`(`id_tenant`,`line_1`,`line_2`,`created_at`) VALUES (%s,%s,%s,%s)"  
        insert_update_delete_many(qry,address)
    except Exception as e:
        print(f"address : {str(e)}")
        
        
    # ---------------------------------------------------------------- Map a customer_group_pk and address_pk ------------------------------------------------------------------        
        
    customer_group_pk_and_address_pk = []
    for line in reader:
        email = line.get('email')
        address1 = line.get('address1')
        address2 = line.get('address2')
        try:
            qry = "SELECT `id_customer_group` FROM `customer_group` WHERE  `email` = %s"
            customer_group_pk = select_one(qry, email)
            qry = "SELECT `id_address` FROM `addresses` WHERE  `line_1` = %s AND `line_2` = %s"
            values = (address1, address2)
            address_pk = select_one(qry, values)
            time = datetime.datetime.now()
            customer_group_pk_and_address_pk.append((15,customer_group_pk, address_pk,time))
        except Exception as e:
            print("Error : " + str(e))
    
    try:
        qry = "INSERT INTO `customer_group_addresses`(`tenant_id`,`id_customer_group`,`id_address`,`created_at`) VALUES (%s,%s,%s,%s)"  
        insert_update_delete_many(qry,address)
    except Exception as e:
        print(f"customer_group_addresses : {str(e)}")
        
    data = {"diffence" :  str(datetime.datetime.now() - start),'message': 'File uploaded successfully'}
    response = make_response(jsonify(data),200)
    response.headers["Content-Type"] = "application/json"
    return response



if __name__ == "__main__":
    app.run(debug=True)