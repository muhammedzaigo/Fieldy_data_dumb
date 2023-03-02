from utils.json_condition import *
from utils.utils import (password_hash, TENANT_ID, SECRET_KEY, DEFAULT_PASSWORD,
                         create_avatar_then_dumb_files_db_and_map_customer_group_thread)
import io
from flask import *
import csv
import datetime
import threading
from utils.query import *

app = Flask(__name__)
app.secret_key = SECRET_KEY


@app.route("/api/customer_group", methods=['POST'])
def customer_group():
    start = datetime.datetime.now()

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})

    file = request.files['file']
    import_sheet = file.read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(import_sheet))
    bulk_insert_id = bulk_insert(select=True)
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
        customer_group.append((name, email, TENANT_ID, time, bulk_insert_id))
        address.append((TENANT_ID, address1, address2, time, bulk_insert_id))

        # emails.append(email)
        # lines.append(address1)

        # single_insert_responce = single_insert(name, email, TENANT_ID, time)
        # single_delete_responce = single_delete(email)

    # bulk_delete_responce = bulk_delete_custemer_group_and_addresses(emails,lines)

    customer_group_id_and_emails = bulk_insert_custemer_group(
        customer_group, bulk_insert_id, insert=True, select=True)
    address_id_and_lines = bulk_insert_addresses(
        address, bulk_insert_id, insert=True, select=True)

    create_avatar_then_dumb_files_db_and_map_customer_group = threading.Thread(
        target=create_avatar_then_dumb_files_db_and_map_customer_group_thread, args=(customer_group_id_and_emails, bulk_insert_id,))
    create_avatar_then_dumb_files_db_and_map_customer_group.start()

    # ---------------------------------------------------------------- Map a customer_group_pk and address_pk ------------------------------------------------------------------

    customer_group_addresses = []  # customer_group_pk and address_pk
    phone_number_and_customer_group = []
    users_data_and_customer_group = []
    id_address = []

    hash_password = password_hash(DEFAULT_PASSWORD)
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
            if email == customer_group_id_and_email[1] and name == customer_group_id_and_email[2]:
                # map customer_group_pk and phone number for phones table
                phone_number_and_customer_group.append(
                    (phone, customer_group_id_and_email[0], TENANT_ID, datetime.datetime.now()))
                # map customer_group_pk and first name and last name for users table
                users_data_and_customer_group.append(
                    (name, first_name, last_name, email, customer_group_id_and_email[0], hash_password, datetime.datetime.now()))

                customer_group_pk_and_address_pk.append(
                    customer_group_id_and_email[0])
                break

        for address_id_and_line in address_id_and_lines:
            if address1 == address_id_and_line[1] and address2 == address_id_and_line[2]:

                address_id = address_id_and_line[0]
                customer_group_pk_and_address_pk.append(address_id)
                id_address.append(address_id)
                break

        customer_group_pk_and_address_pk.insert(0, TENANT_ID)
        customer_group_pk_and_address_pk.insert(3, datetime.datetime.now())
        customer_group_addresses.append(
            tuple(customer_group_pk_and_address_pk))

    customer_group_addresses_all = bulk_insert_customer_group_addresses(
        customer_group_addresses, id_address, insert=True)
    users = bulk_insert_users(users_data_and_customer_group, insert=True)
    phones = bulk_insert_phones(phone_number_and_customer_group, insert=True)

    data = {
        "diffence":  str(datetime.datetime.now() - start),
        'message': 'File uploaded successfully',
    }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response


@app.route("/api/test_api", methods=['POST'])
def test_api():
    start = datetime.datetime.now()

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})

    file = request.files['file']
    import_sheet = file.read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(import_sheet))
    json_format = JSON_CONDITION
    json_count = len(json_format.keys())
    field_names = reader.fieldnames
    splite_field_name_with_json_count = field_names[0:json_count]

    contact_customer_list = []
    organization_customer_list = []
    
    for line in reader:
        field_type = divide_to_field_type_with_json_format(line, splite_field_name_with_json_count, json_format)
        contact_customer_list.append(field_type["contact"])
        organization_customer_list.append(field_type["organization"])

    tables_name = get_save_table_names(json_format)
    contact_customer_list = divide_list_use_table_name_for_save_table(contact_customer_list,tables_name)
    # organization_customer_list = divide_list_use_table_name_for_save_table(organization_customer_list,tables_name)
    bulk_insert_id = bulk_insert(select=True)
    bulk_insert_function(contact_customer_list,TENANT_ID,bulk_insert_id, insert=True)

    data = {
        "diffence":  str(datetime.datetime.now() - start),
        'message': 'File uploaded successfully',
    }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response


def bulk_insert_function(bulk_insert_list,TENANT_ID,bulk_insert_id, insert=False, select=False):
    
    for bulk_insert_table_name,bulk_insert_values in bulk_insert_list.items():
        table_name = bulk_insert_table_name
        column_names = []
        values = []
        for column_name in bulk_insert_values[0]:
            column_names.append(column_name['column_name'])
        # column_names.append('TENANT_ID')
        column_names.append('created_at')
        # column_names.append('bulk_insert_id')
        
        for bulk_insert_value in bulk_insert_values:
            val = []
            for items in bulk_insert_value:
                val.append(items['value'])
            # val.append(TENANT_ID)
            val.append(datetime.datetime.now())
            # val.append(bulk_insert_id)
            values.append(tuple(val))
        bulk_insert_custemer_group_dynamic(table_name,column_names,values,bulk_insert_id,insert=True)
        
            
def bulk_insert_custemer_group_dynamic(table_name,column_names,values,bulk_insert_id,insert=False,select=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if insert:
            make_presentage_s = ",".join(["%s"] * len(values[0]))
            column_names = "({})".format(", ".join("`{}`".format(name) for name in column_names))
            qry = f'''INSERT INTO `{table_name}` {column_names} VALUES ({make_presentage_s})'''
            insert_update_delete_many(qry,values)
        if select:
            qry = ''' SELECT `id_customer_group`,`email`,`name` FROM `customer_group` WHERE `bulk_insert_id` = %s'''
            customer_group_id_and_emails = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"{table_name} : {str(e)}")
    return customer_group_id_and_emails


def divide_list_use_table_name_for_save_table(contact_or_organization_list,tables_names):
    table_name_dict_list = table_name_dict_of_list(tables_names)
    for list_of_dict_items in contact_or_organization_list:
        for list_of_dict_item in list_of_dict_items:
            for key,value in list_of_dict_item.items():      
                table_name_dict_list[key].append(value)    
    return table_name_dict_list


def table_name_dict_of_list(tables_names):
    table_name_dict_of_list = {}
    for table_name in tables_names:
        table_name_dict_of_list[table_name] = []
    return table_name_dict_of_list


def divide_to_field_type_with_json_format(line, field_names, json_format):
    contact_customer_inner_list = []
    organization_customer_inner_list = []
        
    json_format_keys = json_format.keys()
    for index,key in enumerate(json_format_keys):
        user_type = json_format[key]['entity']
        field_name = field_names[index]
        
        if user_type == "contact":
            field_format_return_dict = field_format_dict(json_format[key]['parent'],json_format[key]['table_slug'],line[field_name])
            contact_customer_inner_list.append((field_format_return_dict))  
            
        if user_type == "organization":
            field_format_return_dict = field_format_dict(json_format[key]['parent'],json_format[key]['table_slug'],line[field_name])
            organization_customer_inner_list.append((field_format_return_dict))
    
    contact_customer_inner_list = split_table_name_ways(contact_customer_inner_list)
    organization_customer_inner_list = split_table_name_ways(organization_customer_inner_list)
    return {"contact": contact_customer_inner_list , "organization": organization_customer_inner_list}


def field_format_dict(table_name,column_name,value):
    field_format_dict = {}
    table_name = get_table_name(table_name)
    field_format_dict.update({"table_name":table_name,"column_name":column_name,"value":value})
    return field_format_dict


def get_table_name(table_name):
    if len(table_name) == 0:
        table_name = "customer_group"
    return table_name


def get_save_table_names(json_format):
    table_names = []
    for key in json_format.keys():
        table_name = get_table_name(json_format[key]['parent'])
        table_names.append(table_name)
    return list(set(table_names))
    

def split_table_name_ways(field_format_return_dict):
    responce_list = []
    responce_dict ={}
    for field_name in field_format_return_dict:
        if field_name['table_name'] not in responce_dict:
            responce_dict[field_name['table_name']] = []
        responce_dict[field_name['table_name']].append(field_name)
    responce_list.append(responce_dict)
    return responce_list

# def split_table_name_ways(field_format_return_dict):
#     responce_list = []
#     for field_names in field_format_return_dict:
#         responce_dict ={}
#         for field_name in field_names:
#             if field_name['table_name'] not in responce_dict:
#                 responce_dict[field_name['table_name']] = []
#             responce_dict[field_name['table_name']].append(field_name)
#         responce_list.append(responce_dict)
#     return responce_list


if __name__ == "__main__":
    app.run(debug=True)

