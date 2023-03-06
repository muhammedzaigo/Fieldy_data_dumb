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


@app.route("/api/test_api", methods=['POST'])
def test_api():

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
        field_type = divide_to_field_type_with_json_format(
            line, splite_field_name_with_json_count, json_format)
        contact_customer_list.append(field_type["contact"])
        organization_customer_list.append(field_type["organization"])
        contact_list.append(field_type["contact_list"])
        organization_list.append(field_type["organization_list"])

    tables_name = get_table_names_in_json_condition(json_format)
    contact_customer_list = table_name_use_suparat_all_data(
        contact_customer_list, tables_name)
    organization_customer_list = table_name_use_suparat_all_data(
        organization_customer_list, tables_name)
    bulk_insert_id = get_bulk_insert_id(select=True)
    contact_customer_group_addresess_list = bulk_insert_function(
        contact_customer_list, TENANT_ID, bulk_insert_id, custemer_type="contact_customer")
    organization_customer_group_addresess_list = bulk_insert_function(
        organization_customer_list, TENANT_ID, bulk_insert_id, custemer_type="company_customer")
    users_and_phones_map_list(
        contact_list, contact_customer_group_addresess_list)
    users_and_phones_map_list(
        organization_list, organization_customer_group_addresess_list)

    data = {
        'message': 'File uploaded successfully',
    }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response


def divide_to_field_type_with_json_format(line, field_names, json_format):
    contact_customer_inner_list = []
    organization_customer_inner_list = []

    json_format_keys = json_format.keys()
    for index, key in enumerate(json_format_keys):
        user_type = json_format[key]['entity']
        field_name = field_names[index]
        value = line[field_name]
        if value == ".":
            value = ""
        if user_type == "contact":
            field_format_return_dict = finding_which_data(
                user_type, json_format[key]['parent'], json_format[key]['table_slug'], value)
            contact_customer_inner_list.append((field_format_return_dict))

        if user_type == "organization":
            field_format_return_dict = finding_which_data(
                user_type, json_format[key]['parent'], json_format[key]['table_slug'], value)
            organization_customer_inner_list.append((field_format_return_dict))

    contact_customer_inner_list.append(add_new_field(
        "contact", "customer_group", "customer_type", "contact_customer"))
    organization_customer_inner_list.append(add_new_field(
        "organization", "customer_group", "customer_type", "company_customer"))
    contact_list = contact_customer_inner_list
    organization_list = organization_customer_inner_list
    contact_customer_inner_list = organizing_with_table_name(
        contact_customer_inner_list)
    organization_customer_inner_list = organizing_with_table_name(
        organization_customer_inner_list)

    contaxt = {"contact": contact_customer_inner_list,
               "organization": organization_customer_inner_list,
               "contact_list": contact_list,
               "organization_list": organization_list}
    return contaxt


def add_new_field(user_type, table_name, column_name, value):
    field_format_dict = {}
    field_format_dict.update(
        {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value})
    return field_format_dict


def finding_which_data(user_type, table_name, column_name, value):
    field_format_dict = {}
    table_name = get_table_name(table_name)
    if column_name == "zipcode":
        column_name = "zip_code"
    if table_name == "customer_group" and column_name == "zip_code":
        table_name = "addresses"
    field_format_dict.update(
        {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value})
    return field_format_dict


def get_table_name(table_name):
    if len(table_name) == 0:
        table_name = "customer_group"
    return table_name


def organizing_with_table_name(field_format_return_dict):
    responce_list = []
    responce_dict = {}
    for field_name in field_format_return_dict:
        if field_name['table_name'] not in responce_dict:
            responce_dict[field_name['table_name']] = []
        responce_dict[field_name['table_name']].append(field_name)

    responce_dict["users"] = []
    if "customer_group" in responce_dict.keys():
        user_dict = {"user_type": "", 'table_name': "customer_group",
                     'column_name': 'name', "value": ""}

        for item in responce_dict["customer_group"]:
            if item["user_type"] == "contact":
                if item['column_name'] == "first_name":
                    responce_dict["users"].append(item)
                    user_dict["value"] = item["value"]
                if item['column_name'] == "last_name":
                    responce_dict["users"].append(item)
                    user_dict["value"] += ' '+item["value"]
                if item['column_name'] == "email":
                    responce_dict["users"].append(item)
                user_dict["user_type"] = item['user_type']

            if item["user_type"] == "organization":
                if item['column_name'] == "name":
                    user_dict["value"] = item["value"]
                user_dict["user_type"] = item['user_type']

        if user_dict["user_type"] == "contact":
            responce_dict["customer_group"].append(user_dict)
        responce_dict["users"].append(user_dict)
        responce_dict["customer_group"] = [d for d in responce_dict["customer_group"]
                                           if d['column_name'] not in ('first_name', 'last_name')]
    responce_list.append(responce_dict)
    return responce_list


def get_table_names_in_json_condition(json_format):
    table_names = []
    for key in json_format.keys():
        table_name = get_table_name(json_format[key]['parent'])
        table_names.append(table_name)
    table_names = list(set(table_names))
    if "customer_group" in table_names:
        table_names.append('users')
    return table_names


def table_name_use_suparat_all_data(contact_or_organization_list, tables_names):
    table_name_dict_of_list = all_table_names_convert_dict_of_list(
        tables_names)
    for list_of_dict_items in contact_or_organization_list:
        for list_of_dict_item in list_of_dict_items:
            for key, value in list_of_dict_item.items():
                table_name_dict_of_list[key].append(value)
    return table_name_dict_of_list


def all_table_names_convert_dict_of_list(tables_names):
    table_name_dict_of_list = {}
    for table_name in tables_names:
        table_name_dict_of_list[table_name] = []
    return table_name_dict_of_list


def bulk_insert_function(bulk_insert_list, TENANT_ID, bulk_insert_id, custemer_type=None):
    retrive_customer_group_data_use_bulk_insert_id = []
    retrive_addresses_data_use_bulk_insert_id = []
    for table_name, all_values in bulk_insert_list.items():
        if table_name == "customer_group" or table_name == "addresses":
            get_single_value_for_table_names = all_values[0] if len(
                all_values) != 0 else []
            column_names = get_column_names(
                table_name, get_single_value_for_table_names)
            values = get_values(table_name, TENANT_ID,
                                all_values, bulk_insert_id)
            if table_name == "customer_group":
                bulk_insert(table_name, column_names, values, insert=True)
                retrive_customer_group_data_use_bulk_insert_id = retrive_customer_group_and_addresses_data_use_bulk_insert_id(
                    table_name, bulk_insert_id, custemer_type, select_customer_group=True)
                create_avatar_then_dumb_files_db_and_map_customer_group = threading.Thread(
                    target=create_avatar_then_dumb_files_db_and_map_customer_group_thread, args=(retrive_customer_group_data_use_bulk_insert_id, bulk_insert_id,))
                create_avatar_then_dumb_files_db_and_map_customer_group.start()
            if table_name == "addresses":
                bulk_insert(table_name, column_names, values, insert=True)
                retrive_addresses_data_use_bulk_insert_id = retrive_customer_group_and_addresses_data_use_bulk_insert_id(
                    table_name, bulk_insert_id, select_address=True)
    responce = {
        "retrive_customer_group": retrive_customer_group_data_use_bulk_insert_id,
        "retrive_addresses": retrive_addresses_data_use_bulk_insert_id
    }
    return responce


def get_column_names(table_name, bulk_insert_values):
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


def get_values(table_name, TENANT_ID, bulk_insert_values, bulk_insert_id):
    values = []
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


def bulk_insert(table_name, column_names, values, insert=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if insert:
            make_presentage_s = ",".join(["%s"] * len(values[0]))
            column_names = "({})".format(
                ", ".join("`{}`".format(name) for name in column_names))
            qry = f'''INSERT INTO `{table_name}` {column_names} VALUES ({make_presentage_s})'''
            insert_update_delete_many(qry, values)
    except Exception as e:
        print(f"insert {table_name} : {str(e)}")
    return customer_group_id_and_emails


def retrive_customer_group_and_addresses_data_use_bulk_insert_id(table_name, bulk_insert_id, custemer_type=None, select_customer_group=False, select_address=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if select_customer_group:
            qry = ''' SELECT `id_customer_group`,`email`,`name` FROM `customer_group` WHERE `bulk_insert_id` = %s and `customer_type` = %s'''
            val = (bulk_insert_id, custemer_type)
            customer_group_id_and_emails = select_filter(qry, val)
        if select_address:
            qry = ''' SELECT `id_address`,`line_1`,`line_2` FROM `addresses` WHERE `bulk_insert_id` = %s'''
            customer_group_id_and_emails = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"{table_name} : {str(e)}")
    return customer_group_id_and_emails


def users_and_phones_map_list(sheet_row_ways_contact_or_organization_list, retrive_db_customer_group_or_address_list):
    customer_group_addresses = []
    phone_number_and_customer_group = []
    users_data_and_customer_group = []
    customer_group_id_and_emails = retrive_db_customer_group_or_address_list[
        "retrive_customer_group"]
    address_id_and_lines = retrive_db_customer_group_or_address_list["retrive_addresses"]
    id_address = []
    hash_password = password_hash(DEFAULT_PASSWORD)
    for row in sheet_row_ways_contact_or_organization_list:
        first_name = ""
        last_name = ""
        email = None
        phone = None
        line_1 = None
        line_2 = None
        name = ""
        for value in row:
            if value["column_name"] == "first_name":
                first_name = value["value"]
            if value["column_name"] == "last_name":
                last_name = value["value"]
            if value["column_name"] == "email":
                email = value["value"]
            if value["column_name"] == "line_1":
                line_1 = value["value"]
            if value["column_name"] == "line_2":
                line_2 = value["value"]
            if value["column_name"] == "number":
                phone = value["value"]
        if len(first_name) != 0:
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
            if line_1 == address_id_and_line[1] and line_2 == address_id_and_line[2]:
                address_id = address_id_and_line[0]
                customer_group_pk_and_address_pk.append(address_id)
                id_address.append(address_id)
                break

        customer_group_pk_and_address_pk.insert(0, TENANT_ID)
        customer_group_pk_and_address_pk.insert(3, datetime.datetime.now())
        customer_group_addresses.append(tuple(customer_group_pk_and_address_pk))
        
    bulk_insert_customer_group_addresses(customer_group_addresses, id_address, insert=True)
    bulk_insert_users(users_data_and_customer_group, insert=True)
    bulk_insert_phones(phone_number_and_customer_group, insert=True)
    return


if __name__ == "__main__":
    app.run(debug=True)
