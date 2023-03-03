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
    contact_list = []
    organization_list = []
    
    for line in reader:
        field_type = divide_to_field_type_with_json_format(line, splite_field_name_with_json_count, json_format)
        contact_customer_list.append(field_type["contact"])
        organization_customer_list.append(field_type["organization"])
        contact_list.append(field_type["contact_list"])
        organization_list.append(field_type["organization_list"])
    tables_name = get_save_table_names(json_format)
    contact_customer_list = divide_list_use_table_name_for_save_table(contact_customer_list,tables_name)
    organization_customer_list = divide_list_use_table_name_for_save_table(organization_customer_list,tables_name)
    bulk_insert_id = bulk_insert(select=True)
    bulk_insert_function(contact_customer_list,contact_list,TENANT_ID,bulk_insert_id)
    # bulk_insert_function(organization_customer_list,organization_list,TENANT_ID,bulk_insert_id,)

    data = {
        "diffence":  str(datetime.datetime.now() - start),
        'message': 'File uploaded successfully',
        "organization_customer_list":organization_customer_list
    }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response


def bulk_insert_function(bulk_insert_list,single_deatails,TENANT_ID,bulk_insert_id):
    customer_group_id_and_emails = []
    address_id_and_lines = []
    for bulk_insert_table_name,bulk_insert_values in bulk_insert_list.items():
        table_name = bulk_insert_table_name
        if table_name == "customer_group" or table_name == "addresses":
            bulk_insert_values_for_table = bulk_insert_values[0] if len(bulk_insert_values) !=0 else []
            column_names = table_name_for_bulk_insert(table_name,bulk_insert_values_for_table)
            values = table_values_for_bulk_insert(table_name,TENANT_ID,bulk_insert_values,bulk_insert_id)
            if table_name == "customer_group":      
                customer_group_id_and_emails = bulk_insert_custemer_group_dynamic(table_name,column_names,values,bulk_insert_id,insert=True,select_customer_group=True)
                create_avatar_then_dumb_files_db_and_map_customer_group = threading.Thread(
                target=create_avatar_then_dumb_files_db_and_map_customer_group_thread, args=(customer_group_id_and_emails, bulk_insert_id,))
                create_avatar_then_dumb_files_db_and_map_customer_group.start()
            if table_name == "addresses":
                address_id_and_lines = bulk_insert_custemer_group_dynamic(table_name,column_names,values,bulk_insert_id,insert=True,select_address=True)
    # for bulk_insert_table_name,bulk_insert_values in bulk_insert_list.items():
    #     table_name = bulk_insert_table_name
    #     if table_name == "phones":
    #         users_and_phones_map_list(address_id_and_lines,customer_group_id_and_emails,single_deatails)

   
def users_and_phones_map_list(address_list,customer_group_lists,single_deatails):
    for values in single_deatails:
        first_name = None
        last_name = None
        email = None
        phone = None
        line_1 = None
        line_2 = None
        for value in values:
            if  value["column_name"] == "first_name":
                first_name = value["value"]
            if  value["column_name"] == "last_name":
                last_name = value["value"]
            if  value["column_name"] == "email":
                email = value["value"]
            if  value["column_name"] == "line_1":
                line_1 = value["value"]
            if  value["column_name"] == "line_2":
                line_2 = value["value"]      
                
        name = first_name+" "+last_name 
        for customer_group_list in customer_group_lists:
            if email == customer_group_list[1] and name == customer_group_list[2]:
                print(customer_group_list)
            # break
                
def table_name_for_bulk_insert(table_name,bulk_insert_values):
    column_names = []
    for column_name in bulk_insert_values:
        column_names.append(column_name['column_name'])
    if table_name == "addresses":
        column_names.append("id_tenant")
    if table_name == "customer_group" or table_name == "phones":
        column_names.append("tenant_id")
    column_names.append("created_at")
    if table_name == "customer_group" or table_name == "addresses":
        column_names.append("bulk_insert_id")
    return column_names
            
def table_values_for_bulk_insert(table_name,TENANT_ID,bulk_insert_values,bulk_insert_id):
    values  =[]
    for bulk_insert_value in bulk_insert_values:
        val = []
        for items in bulk_insert_value:
            val.append(items['value'])
        val.append(TENANT_ID)
        val.append(datetime.datetime.now())
        if table_name == "customer_group" or table_name == "addresses":
            val.append(bulk_insert_id)
        values.append(tuple(val))    
    return values  
            
            
def bulk_insert_custemer_group_dynamic(table_name,column_names,values,bulk_insert_id,insert=False,select_customer_group=False,select_address=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if insert:
            make_presentage_s = ",".join(["%s"] * len(values[0]))
            column_names = "({})".format(", ".join("`{}`".format(name) for name in column_names))
            qry = f'''INSERT INTO `{table_name}` {column_names} VALUES ({make_presentage_s})'''
            insert_update_delete_many(qry,values)
        if select_customer_group:
            qry = ''' SELECT `id_customer_group`,`email`,`name` FROM `customer_group` WHERE `bulk_insert_id` = %s'''
            customer_group_id_and_emails = select_filter(qry, bulk_insert_id)
        if select_address:
            qry = ''' SELECT `id_address`,`line_1`,`line_2` FROM `addresses` WHERE `bulk_insert_id` = %s'''
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
    table_name_dict_of_list["users"] = []
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
    contact_list = contact_customer_inner_list
    organization_list = organization_customer_inner_list
    contact_customer_inner_list = split_table_name_ways(contact_customer_inner_list)
    organization_customer_inner_list = split_table_name_ways(organization_customer_inner_list)
    
    contaxt = {"contact": contact_customer_inner_list ,
               "organization": organization_customer_inner_list,
               "contact_list":contact_list,
               "organization_list":organization_list}
    return contaxt


def field_format_dict(table_name,column_name,value):
    field_format_dict = {}
    table_name = get_table_name(table_name)
    if table_name == "addresses" and column_name == "zipcode":
        column_name = "zip_code"
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
    responce_dict["users"] = []
    if "customer_group" in responce_dict.keys():
        new_dict = {'table_name': "customer_group", 'column_name': 'name',"value": ""}
        for item in responce_dict["customer_group"]:
            if item['column_name'] == "first_name":
                responce_dict["users"].append(item)
                new_dict["value"] = item["value"]
            if item['column_name'] == "last_name":
                responce_dict["users"].append(item)
                new_dict["value"] +=' '+item["value"]
            if item['column_name'] == "email":
                responce_dict["users"].append(item)
        responce_dict["customer_group"].append(new_dict)
        responce_dict["users"].append(new_dict)
        responce_dict["customer_group"] = [d for d in responce_dict["customer_group"] if d['column_name'] not in ('first_name', 'last_name')]
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

