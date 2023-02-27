
from database_connection import *
import io
from flask import *
import csv
import datetime

app = Flask(__name__)
app.secret_key = "f#6=zf!2=n@ed-=6g17&k4e4fl#d4v&l*v6q5_6=8jz1f98v#"
UPLOAD_FOLDER = "media"

from utils import random_string,create_avatar




@app.route("/api/customer_group", methods=['POST'])
def customer_group():
    start = datetime.datetime.now()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})
    file = request.files['file']
    import_sheet = file.read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(import_sheet))

    
    bulk_insert_id : int = 0
    try:
        # qry = '''INSERT INTO `bulk_insert`(`bulk_insert_number`,`created_at`) VALUES (%s,%s)'''
        # val = (1, datetime.datetime.now())
        # last_row_id = insert_update_delete(qry,val)
        qry = '''SELECT `id` FROM `bulk_insert` ORDER BY id DESC LIMIT 1'''
        last_row_id = select_all(qry)
        bulk_insert_id = last_row_id[0][0]
    except Exception as e:
        print(f"bulk_insert : {str(e)}")
        
    
    customer_group = []
    address = []
    emails = []
    lines = []
    tenant_id = 15
    
    for line in reader:
        first_name = line.get('first_name')
        last_name = line.get('last_name')
        email = line.get('email')
        address1 = line.get('address1')
        address2 = line.get('address2')
        name = first_name+" "+last_name
        time = datetime.datetime.now()
        avatar = create_avatar(name)
        customer_group.append((name, email, tenant_id, time, bulk_insert_id))
        address.append((tenant_id, address1, address2, time, bulk_insert_id))
        emails.append(email)
        lines.append(address1)

    # ---------------------------------------------------------------- single Insert ------------------------------------------------------------------

        # try:
        #     qry = "INSERT INTO `customer_group`(`name`,`email`,`tenant_id`,`created_at`) VALUES (%s,%s,%s,%s)"
        #     val = (name,email,15,time)
        #     return_val = insert_update_delete(qry, val)
        # except Exception as e:
        #     print(str(e))

    # ---------------------------------------------------------------- single Delete ------------------------------------------------------------------

        # try:
        #     qry = "DELETE FROM `customer_group` WHERE  `email` = %s"
        #     insert_update_delete(qry, email)
        # except Exception as e:
        #     print(str(e))

    # ---------------------------------------------------------------- Bulk Delete ------------------------------------------------------------------
    # try:
    #     qry = "DELETE FROM `customer_group` WHERE  `email` IN (%s) "
    #     insert_update_delete_many(qry, emails)
    #     qry = "DELETE FROM `addresses` WHERE  `line_1` IN (%s)"
    #     insert_update_delete_many(qry, lines)
    # except Exception as e:
    #     print(str(e))

    # ---------------------------------------------------------------- Bulk Insert ------------------------------------------------------------------
    
        # ------------------------------------- Bulk Insert customer_group -------------------------------------
    
    customer_group_id_and_emails : tuple = ()
    try:
        qry = '''INSERT INTO `customer_group`(`name`,`email`,`tenant_id`,`created_at`,`bulk_insert_id`) VALUES (%s,%s,%s,%s,%s)'''
        insert_update_delete_many(qry, customer_group)
        qry = ''' SELECT `id_customer_group`,`email` FROM `customer_group` WHERE `bulk_insert_id` = %s'''
        customer_group_id_and_emails = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"customer_group : {str(e)}")
        
        # ------------------------------------- Bulk Insert addresses -------------------------------------

    address_id_and_lines : tuple = ()
    try:
        qry = "INSERT INTO `addresses`(`id_tenant`,`line_1`,`line_2`,`created_at`,`bulk_insert_id`) VALUES (%s,%s,%s,%s,%s)"
        insert_update_delete_many(qry,address)
        qry = ''' SELECT `id_address`,`line_1`,`line_2` FROM `addresses` WHERE `bulk_insert_id` = %s'''
        address_id_and_lines = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"address : {str(e)}")
        
        
        
    # ---------------------------------------------------------------- Map a customer_group_pk and address_pk ------------------------------------------------------------------
        
        
    customer_group_addresses = [] # customer_group_pk and address_pk
    phone_number_and_customer_group_pk = []
    users_data_and_customer_group_pk = []
    
    id_address = []
    reader = csv.DictReader(io.StringIO(import_sheet))
    for line in reader:
        email = line.get('email')
        address1 = line.get('address1')
        address2 = line.get('address2')
        phone = line.get('phone')
        first_name = line.get('first_name')
        last_name = line.get('last_name')
        name = first_name+" "+last_name
        
        customer_group_pk_and_address_pk = []

        for customer_group_id_and_email in customer_group_id_and_emails:
            if email == customer_group_id_and_email[1]:
                
                # map customer_group_pk and phone number for phones table
                phone_number_and_customer_group_pk.append((phone,customer_group_id_and_email[0],tenant_id,datetime.datetime.now()))
                # map customer_group_pk and first name and last name for users table
                users_data_and_customer_group_pk.append((name,first_name,last_name,email,customer_group_id_and_email[0],random_string(10),datetime.datetime.now()))
                
                customer_group_pk_and_address_pk.append(customer_group_id_and_email[0])
                break
                
        for address_id_and_line in address_id_and_lines:
            if address1 == address_id_and_line[1] and address2 == address_id_and_line[2]:
    
                address_id = address_id_and_line[0]
                customer_group_pk_and_address_pk.append(address_id)
                id_address.append(address_id)
                break
            
        customer_group_pk_and_address_pk.insert(0,15)
        customer_group_pk_and_address_pk.insert(3,datetime.datetime.now())
        customer_group_addresses.append(tuple(customer_group_pk_and_address_pk))
        

    customer_group_addresses_all : tuple = ()
    try:
        qry = "INSERT INTO `customer_group_addresses`(`tenant_id`,`id_customer_group`,`id_address`,`created_at`) VALUES (%s,%s,%s,%s)"
        insert_update_delete_many(qry,customer_group_addresses)
        # qry = "DELETE FROM `customer_group_addresses` WHERE `id_address` IN (%s) "
        # insert_update_delete_many(qry, id_address)
        # qry = '''  SELECT * FROM `customer_group_addresses`'''
        # customer_group_addresses_all = select_all(qry)
    except Exception as e:
        print(f"customer_group_addresses : {str(e)}")


    users : tuple = ()
    try:
        qry = "INSERT INTO `users`(`name`,`first_name`,`last_name`,`email`,`id_customer_group`,`password`,`created_at`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        insert_update_delete_many(qry,users_data_and_customer_group_pk)
        # qry = '''SELECT `email`,`id_customer_group` FROM `users`'''
        # users = select_all(qry)
    except Exception as e:
        print(f"users : {str(e)}")


    phones : tuple = ()
    try:
        qry = "INSERT INTO `phones`(`number`,`phoneable_id`,`tenant_id`,`created_at`) VALUES (%s,%s,%s,%s)"
        insert_update_delete_many(qry,phone_number_and_customer_group_pk)
        # qry = '''  SELECT `number`,`phoneable_id`  FROM `phones`'''
        # phones = select_all(qry)
    except Exception as e:
        print(f"phones : {str(e)}")

    data = {
            "diffence":  str(datetime.datetime.now() - start),
            'message': 'File uploaded successfully',
            # "customer_group_id_and_emails":customer_group_id_and_emails,
            # "address_id_and_lines":address_id_and_lines,
            # "customer_group_addresses" : customer_group_addresses,
            # "customer_group_addresses_all" : customer_group_addresses_all,
            # "phone_number_and_customer_group_pk":phone_number_and_customer_group_pk,
            # "users_data_and_customer_group_pk":users_data_and_customer_group_pk,
            # "users":users,
            # "phones":phones
            }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response




if __name__ == "__main__":
    app.run(debug=True)
