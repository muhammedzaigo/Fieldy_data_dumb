from utils.utils import *
from utils.query import *
from dotenv import load_dotenv
import pandas as pd
import io
from flask import *
import csv
import datetime
import threading
# from api.xlsx_to_csv import xlsx_convert_csv
# from api.bulk_insert import bulk_insert
from flask_mail import Mail, Message
from template.email import email_template

app = Flask(__name__)
load_dotenv()
# app.register_blueprint(xlsx_convert_csv, url_prefix='/api')
# app.register_blueprint(bulk_insert, url_prefix='/api')

app.secret_key = str(os.getenv('SECRET_KEY'))

app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USERNAME'] = str(os.getenv('MAIL_USERNAME'))
app.config['MAIL_PASSWORD'] = str(os.getenv('MAIL_PASSWORD'))
mail = Mail(app)


def send_email(count, file_url, logo_url, target_email, filename=None):
    with app.app_context():
        try:
            msg = Message('Feildy Message', sender=str(os.getenv('MAIL_SENDER')),
                          recipients=[target_email])
            with app.open_resource(file_url) as csv_file:
                msg.attach(filename=filename,
                           content_type="text/csv", data=csv_file.read())
            msg.html = email_template(count=count)
            mail.send(msg)
            return 'Email sent!'
        except Exception as e:
            return f" email : {str(e)}"


@app.route("/api/bulk_import", methods=['POST'])
def bulk_import_api():
    if request.method == 'POST':

        if 'file' not in request.files:
            return make_response(jsonify({'message': 'No file uploaded'}), 400)

        file = request.files['file']
        TENANT_ID = request.form.get('tanant_id', None)
        json_format = request.form.get('json_format', None)
        target_email = request.form.get('target_email', None)
        created_by = 1
        if TENANT_ID == None or json_format == None:
            return make_response(jsonify({'message': 'tanant_id or json_format is required'}), 400)

        import_sheet = file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(import_sheet))
        json_format = json.loads(json_format)
        currect_json_map_and_which_user_type_check = currect_json_map_and_which_user_type(
            json_format)
        json_format = currect_json_map_and_which_user_type_check["json_format"]
        which_user = currect_json_map_and_which_user_type_check["user_type"]
        json_count = len(json_format.keys())
        field_names = reader.fieldnames
        splite_field_name_with_json_count = field_names[0:json_count]

        contact_customer_list = []
        organization_customer_list = []
        row_ways_contact_list = []
        row_ways_organization_list = []
        invalid_data = []
        skip_data = []

        for line_index, line in enumerate(reader, 1):
            field_type = divide_to_field_type_with_json_format(
                line_index, line, splite_field_name_with_json_count, json_format, which_user)
            if len(field_type["contact"]) != 0:
                contact_customer_list.append(field_type["contact"])
            if len(field_type["organization"]) != 0:
                organization_customer_list.append(field_type["organization"])
            if len(field_type["row_ways_contact_list"]) != 0:
                row_ways_contact_list.append(
                    field_type["row_ways_contact_list"])
            if len(field_type["row_ways_organization_list"]) != 0:
                row_ways_organization_list.append(
                    field_type["row_ways_organization_list"])
            if len(field_type["invalid_data"]) != 0:
                invalid_data.append(field_type["invalid_data"])
            if len(field_type["skip_data"]) != 0:
                skip_data.append(field_type["skip_data"])
            # break
        tables_name = get_table_names_in_json_condition(json_format)
        bulk_insert_id = get_bulk_insert_id(select=True, insert=True)

        context = {
            "TENANT_ID": TENANT_ID,
            "bulk_insert_id": bulk_insert_id,
            "which_user": which_user,
            "created_by": created_by,
        }

        if which_user == CONTACT:
            contact_customer_list = table_name_use_suparat_all_data(
                contact_customer_list, tables_name)
            context.update({"custemer_type": "contact_customer"})
            bulk_insert_using_bulk_type(
                context, contact_customer_list, row_ways_contact_list)
        if which_user == ORGAZANAIZATION:
            organization_customer_list = table_name_use_suparat_all_data(
                organization_customer_list, tables_name)
            context.update({"custemer_type": "company_customer"})
            bulk_insert_using_bulk_type(
                context, organization_customer_list, row_ways_organization_list)

        if target_email:
            send_mail = threading.Thread(target=send_mail_skip_data_and_invalid_data_convert_to_csv, args=(
                splite_field_name_with_json_count, skip_data, invalid_data, target_email))
            send_mail.start()

        data = {
            'message': 'File imported successfully',
            # "organization_customer_list":organization_customer_list
     

        }
        response = make_response(jsonify(data), 200)
        response.headers["Content-Type"] = "application/json"
        return response


# -------------------------------- step 1 --------------------------------

def divide_to_field_type_with_json_format(line_index, line, field_names, json_format, which_user):
    contact_customer_list = []
    organization_customer_list = []
    organized_organization_customer_list = []
    organized_contact_customer_list = []
    invalid_data = []
    skip_data = {}

    json_format_keys = json_format.keys()
    for column_index, key in enumerate(json_format_keys):
        user_type = json_format[key]['entity']
        table_name = json_format[key]['parent']
        column_name = json_format[key]['table_slug']
        validation = json_format[key]['validation']
        field_type = json_format[key]['field_type']

        field_name = field_names[column_index]
        value = line[field_name]
        if value == ".":
            value = ""

        if user_type == "contact":
            field_format_return_dict = finding_which_data(line_index,
                                                          user_type, table_name, column_name, validation, field_type, value, column_index)
            if field_format_return_dict["valid"]:
                contact_customer_list.append((field_format_return_dict))
            else:
                invalid_data.append((field_format_return_dict))
                field_format_return_dict_copy = field_format_return_dict.copy()
                field_format_return_dict_copy["value"] = ""
                contact_customer_list.append(
                    (field_format_return_dict_copy))

        if user_type == "organization":
            field_format_return_dict = finding_which_data(line_index,
                                                          user_type, table_name, column_name, validation, field_type, value, column_index)
            if field_format_return_dict["valid"]:
                organization_customer_list.append(
                    (field_format_return_dict))
            else:
                invalid_data.append((field_format_return_dict))
                field_format_return_dict_copy = field_format_return_dict.copy()
                field_format_return_dict_copy["value"] = ""
                organization_customer_list.append(
                    (field_format_return_dict_copy))

    add_new_field = add_new_field_based_on_user_type(
        line_index, contact_customer_list, organization_customer_list, which_user)
    contact_customer_list = add_new_field["contact_customer_list"]
    organization_customer_list = add_new_field["organization_customer_list"]

    if which_user == CONTACT:

        required = check_have_required_field(
            contact_customer_list, "first_name", "line_1")
        if required:
            organized_contact_customer_list = organizing_with_table_name(line_index,
                                                                         contact_customer_list)
        else:
            skip_data.update({"contact": contact_customer_list})

    if which_user == ORGAZANAIZATION:

        required = check_have_required_field(
            organization_customer_list, "name", "line_1")
        if required:
            organized_organization_customer_list = organizing_with_table_name(line_index,
                                                                              organization_customer_list)
        else:
            skip_data.update(
                {"organization": organization_customer_list})

    contaxt = {"contact": organized_contact_customer_list,
               "organization": organized_organization_customer_list,
               "row_ways_contact_list": contact_customer_list,
               "row_ways_organization_list": organization_customer_list,
               "invalid_data": invalid_data,
               "skip_data": skip_data
               }
    return contaxt


def check_have_required_field(customer_list, name, address):
    required_name = False
    required_line_1 = False
    for customer in customer_list:
        if customer["column_name"] == name:
            required_name = len(customer["value"]) != 0
        if customer["column_name"] == address:
            required_line_1 = len(customer["value"]) != 0
        if required_name and required_line_1:
            break
    return True if required_line_1 and required_name else False


def add_new_field_based_on_user_type(line_index, contact_customer_list, organization_customer_list, which_user):
    if which_user == CONTACT:
        contact_customer_list = add_new_field_in_contact(
            contact_customer_list, line_index)

    if which_user == ORGAZANAIZATION:
        organization_customer_list = add_new_field_in_organization(
            organization_customer_list, line_index)

    return {
        "contact_customer_list": contact_customer_list,
        "organization_customer_list": organization_customer_list
    }


def add_new_field_in_contact(contact_customer_list, line_index):
    contact_customer_list.append(add_new_field(
        "contact", "customer_group", "customer_type", "contact_customer", line_index))
    contact_customer_list = common_fields(
        contact_customer_list, "contact", line_index)
    return contact_customer_list


def add_new_field_in_organization(organization_customer_list, line_index):
    organization_customer_list.append(add_new_field(
        "organization", "customer_group", "customer_type", "company_customer", line_index))
    organization_customer_list = common_fields(
        organization_customer_list, "organization", line_index)
    return organization_customer_list


def common_fields(customer_list, user_type, line_index):
    customer_list.append(add_new_field(
        user_type, "customer_group", "type", "customer_company", line_index))
    return customer_list


def add_new_field(user_type, table_name, column_name, value, line_index):
    field_format_dict = {}
    field_format_dict.update(
        {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value, "valid": True, "line_number": line_index})
    return field_format_dict


def finding_which_data(line_index, user_type, table_name, column_name, validation, field_type, value, column_index):
    field_format_dict = {}
    valid = check_validation(validation, field_type, value)
    if valid:
        field_format_dict.update(
            {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value, "valid": valid, "line_number": line_index, "column_number": column_index})
    else:
        field_format_dict.update(
            {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": f"{value} not valid {column_name}", "valid": valid, "line_number": line_index, "column_number": column_index})
    return field_format_dict


def check_validation(validation, field_type, value):
    valid = False
    min = int(validation["min"]) if len(validation["min"]) != 0 else 1
    max = int(validation["max"]) if len(validation["max"]) != 0 else 256
    if field_type == "alpha_numeric":
        valid = is_valid_alphanumeric(value, min, max)
    if field_type == "all_characters":
        valid = is_all_characters(value, min, max)
    if field_type == "website":
        valid = is_valid_url(value, min, max)
    if field_type == "number":
        valid = is_valid_phone_number(value, min, max)
    if field_type == "email":
        valid = is_valid_email(value, min, max)
    return valid


def organizing_with_table_name(line_index, field_format_return_dict):
    responce_list = []
    responce_dict = {}
    if len(field_format_return_dict) != 0:
        for field_name in field_format_return_dict:
            if field_name['table_name'] not in responce_dict:
                responce_dict[field_name['table_name']] = []
            responce_dict[field_name['table_name']].append(field_name)
        responce_dict = split_customer_group_for_user(
            responce_dict, line_index)
        responce_list.append(responce_dict)
    return responce_list


def split_customer_group_for_user(responce_dict, line_index):
    if "users" not in responce_dict.keys():
        responce_dict["users"] = []

    if "customer_group" in responce_dict.keys():
        user_table_name_dict = {"user_type": "", 'table_name': "customer_group",
                                'column_name': 'name', "value": "", "valid": True, "line_number": line_index}

        for item in responce_dict["customer_group"]:
            if item["user_type"] == "contact":
                if item['column_name'] == "first_name":
                    responce_dict["users"].append(item)
                    user_table_name_dict["value"] = item["value"]
                if item['column_name'] == "last_name":
                    responce_dict["users"].append(item)
                    user_table_name_dict["value"] += ' '+item["value"]
                if item['column_name'] == "email":
                    responce_dict["users"].append(item)
                if item['column_name'] == "job_title":
                    responce_dict["users"].append(item)
                user_table_name_dict["user_type"] = item['user_type']

            if item["user_type"] == "organization":
                if item['column_name'] == "name":
                    user_table_name_dict["value"] = item["value"]
                if item['column_name'] == "first_name":
                    responce_dict["users"].append(item)
                if item['column_name'] == "last_name":
                    responce_dict["users"].append(item)
                user_table_name_dict["user_type"] = item['user_type']

        # remove first_name and last_name field and add name field
        if user_table_name_dict["user_type"] == "contact":
            responce_dict["customer_group"].append(user_table_name_dict)

        responce_dict["customer_group"] = [d for d in responce_dict["customer_group"]
                                           if d['column_name'] not in ('first_name', 'last_name', 'job_title')]
        # add name field in users dictionary
        responce_dict["users"].append(user_table_name_dict)
    return responce_dict
# --------------------------------step 2 --------------------------------


def send_mail_skip_data_and_invalid_data_convert_to_csv(field_names, skip_data, invalid_data, target_email):
    field_names.insert(0, "line Number")
    if len(skip_data) != 0:
        send_mail_skip_data = threading.Thread(
            target=send_skipped_data, args=(field_names, skip_data, target_email))
        send_mail_skip_data.start()
    if len(invalid_data) != 0:
        send_mail_invalid_data = threading.Thread(
            target=send_invalid_data, args=(field_names, invalid_data, target_email))
        send_mail_invalid_data.start()
    return


def send_skipped_data(field_names, skip_data, target_email):
    all_data_list = []
    for data in skip_data:
        single_line_data = [""] * len(field_names)
        line_number = 0
        for key, values in data.items():
            for value in values:
                if "column_number" in value.keys():
                    single_line_data[value["column_number"]+1] = value["value"]
                line_number = value["line_number"]
        single_line_data[0] = line_number
        all_data_list.append(single_line_data)
    csv_name = f"skipped_data_"+datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        df = pd.DataFrame(all_data_list, columns=field_names)
        df.to_csv(f'invalid_data_sheets/{csv_name}.csv', index=False)
    except Exception as e:
        print("Error", str(e))
    send_email(count=10, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="", target_email=target_email, filename=f"{csv_name}.csv")


def send_invalid_data(field_names, invalid_data, target_email):
    invalid_data_list = []
    for data in invalid_data:
        single_line_data = [""] * len(field_names)
        line_number = 0
        for d in data:
            if "column_number" in d.keys():
                single_line_data[d["column_number"]+1] = d["value"]
            line_number = d["line_number"]
        single_line_data[0] = line_number
        invalid_data_list.append(single_line_data)
    csv_name = f"invalid_data_"+datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        df = pd.DataFrame(invalid_data_list, columns=field_names)
        df.to_csv(f'invalid_data_sheets/{csv_name}.csv', index=False)
    except Exception as e:
        print("Error", str(e))
    send_email(count=10, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="", target_email=target_email, filename=f"{csv_name}.csv")

# --------------------------------step 3  --------------------------------


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


# --------------------------------step 4--------------------------------


def bulk_insert_using_bulk_type(context, customer_list=[], row_ways_customer_list=[]):
    TENANT_ID = context["TENANT_ID"]
    bulk_insert_id = context["bulk_insert_id"]
    created_by = context["created_by"]
    custemer_type = context["custemer_type"]
    which_user = context["which_user"]

    customer_group_addresess_list = bulk_insert_function(
        customer_list, TENANT_ID, bulk_insert_id, created_by, custemer_type)
    thread = threading.Thread(target=users_and_phones_and_customer_group_addresess_mapping, args=(
        row_ways_customer_list, customer_group_addresess_list, TENANT_ID, which_user, created_by))
    thread.start()
    return "Successfully"


def bulk_insert_function(bulk_insert_list, TENANT_ID, bulk_insert_id, created_by, custemer_type=None):
    retrive_customer_group_data_use_bulk_insert_id = []
    retrive_addresses_data_use_bulk_insert_id = []

    for table_name, all_values in bulk_insert_list.items():
        if table_name in ["customer_group", "addresses", "branch_addresses"]:
            get_single_value_for_table_names = all_values[0] if len(
                all_values) != 0 else []
            column_names = get_column_names(
                table_name, get_single_value_for_table_names)
            values = get_column_values(
                table_name, TENANT_ID, all_values, bulk_insert_id, created_by)
            if len(values) != 0:
                if table_name == "customer_group":
                    bulk_insert_dynamic(
                        table_name, column_names, values, insert=True)
                if table_name in ["addresses", "branch_addresses"]:
                    if table_name == "branch_addresses":
                        table_name = "addresses"
                    bulk_insert_dynamic(
                        table_name, column_names, values, insert=True)

    retrive_customer_group_data_use_bulk_insert_id = retrive_customer_group_and_addresses_data_use_bulk_insert_id(
        "customer_group", bulk_insert_id, custemer_type, select_customer_group=True)
    retrive_addresses_data_use_bulk_insert_id = retrive_customer_group_and_addresses_data_use_bulk_insert_id(
        "addresses", bulk_insert_id, select_address=True)

    if len(retrive_customer_group_data_use_bulk_insert_id) != 0:
        create_avatar_then_dumb_files_db_and_map_customer_group = threading.Thread(
            target=create_avatar_then_dumb_files_db_and_map_customer_group_thread, args=(
                retrive_customer_group_data_use_bulk_insert_id, bulk_insert_id, TENANT_ID))
        create_avatar_then_dumb_files_db_and_map_customer_group.start()

    responce = {
        "retrive_customer_group": retrive_customer_group_data_use_bulk_insert_id,
        "retrive_addresses": retrive_addresses_data_use_bulk_insert_id,
    }
    return responce


def users_and_phones_and_customer_group_addresess_mapping(row_ways_customer_list, customer_group_addresess_list, TENANT_ID, which_user, created_by):
    customer_group_addresses = []
    phone_number_and_customer_group = []
    users_data_and_customer_group = []
    customer_group_id_and_emails = customer_group_addresess_list["retrive_customer_group"]
    address_id_and_lines = customer_group_addresess_list["retrive_addresses"]
    role_id = retrive_role_id(TENANT_ID, select=True)
    hash_password = password_hash(DEFAULT_PASSWORD)

    for row in row_ways_customer_list:
        customer_first_name = ""
        customer_last_name = ""
        customer_email = None
        customer_name = None
        phone = None
        addresses_line_1 = None
        addresses_line_2 = None
        branch_addresses_line_1 = None
        branch_addresses_line_2 = None
        users_first_name = None
        users_last_name = None
        users_email = None
        users_phone = None
        users_job_title = None

        for value in row:
            if value["column_name"] == "first_name" and value["table_name"] == "customer_group":
                customer_first_name = value["value"]
            if value["column_name"] == "last_name" and value["table_name"] == "customer_group":
                customer_last_name = value["value"]
            if value["column_name"] == "email" and value["table_name"] == "customer_group":
                customer_email = value["value"]

            if value["column_name"] == "name" and value["table_name"] == "customer_group":
                customer_name = value["value"]

            if value["column_name"] == "line_1" and value["table_name"] == "addresses":
                addresses_line_1 = value["value"]
            if value["column_name"] == "line_2" and value["table_name"] == "addresses":
                addresses_line_2 = value["value"]

            if value["column_name"] == "line_1" and value["table_name"] == "branch_addresses":
                branch_addresses_line_1 = value["value"]
            if value["column_name"] == "line_2" and value["table_name"] == "branch_addresses":
                branch_addresses_line_2 = value["value"]

            if value["column_name"] == "number" and value["table_name"] == "phones":
                phone = value["value"]

            if value["column_name"] == "first_name" and value["table_name"] == "users":
                users_first_name = value["value"]
            if value["column_name"] == "last_name" and value["table_name"] == "users":
                users_last_name = value["value"]
            if value["column_name"] == "email" and value["table_name"] == "users":
                users_email = value["value"]
            if value["column_name"] == "phone" and value["table_name"] == "users":
                users_phone = value["value"]
            if value["column_name"] == "job_title" and value["table_name"] == "users":
                users_job_title = value["value"]

        if customer_name is None and len(customer_first_name) != 0:
            customer_name = customer_first_name+" "+customer_last_name

        customer_group_pk_and_address_pk = []
        customer_group_pk_and_address_pk_branch_address = []
        

        for customer_group_id_and_email in customer_group_id_and_emails:
            if customer_email == customer_group_id_and_email[1] and customer_name == customer_group_id_and_email[2]:

                # map customer_group_pk and phone number for phones table
                if len(phone) != 0:
                    phone_number_and_customer_group.append(
                        (phone, customer_group_id_and_email[0], TENANT_ID, datetime.datetime.now()))
                # map customer_group_pk and first name and last name for users table
                if which_user == ORGAZANAIZATION:
                    if users_first_name != None or users_last_name != None:
                        users_data_and_customer_group.append(
                            ("", users_first_name, users_last_name, users_email, users_phone, users_job_title, customer_group_id_and_email[0], TENANT_ID, role_id, created_by, hash_password, datetime.datetime.now()))
                    users_data_and_customer_group.append(
                        (customer_name, "", "", customer_email, "", "", customer_group_id_and_email[0], TENANT_ID, role_id, created_by, hash_password, datetime.datetime.now()))
                else:
                    users_data_and_customer_group.append(
                        (customer_name, customer_first_name, customer_last_name, customer_email, users_phone, users_job_title, customer_group_id_and_email[0], TENANT_ID, role_id, created_by, hash_password, datetime.datetime.now()))
                
                if addresses_line_1  is not None:
                    customer_group_pk_and_address_pk.append(customer_group_id_and_email[0])
                if branch_addresses_line_1 is not None:
                    customer_group_pk_and_address_pk_branch_address.append(customer_group_id_and_email[0])
                break

        if addresses_line_1 is not None:
            for address_id_and_line in address_id_and_lines:
                if addresses_line_1 == address_id_and_line[1] and addresses_line_2 == address_id_and_line[2]:
                    customer_group_pk_and_address_pk.append(address_id_and_line[0])
                    break
            customer_group_pk_and_address_pk.insert(0, TENANT_ID)
            customer_group_pk_and_address_pk.insert(3, datetime.datetime.now())
            customer_group_addresses.append((customer_group_pk_and_address_pk))
        
        if branch_addresses_line_1 is not None:
            for address_id_and_line in address_id_and_lines:
                if branch_addresses_line_1 == address_id_and_line[1] and branch_addresses_line_2 == address_id_and_line[2]:
                    customer_group_pk_and_address_pk_branch_address.append(address_id_and_line[0])
                    break
            customer_group_pk_and_address_pk_branch_address.insert(0, TENANT_ID)
            customer_group_pk_and_address_pk_branch_address.insert(3, datetime.datetime.now())
            customer_group_addresses.append((customer_group_pk_and_address_pk_branch_address))
            

    bulk_insert_customer_group_addresses(customer_group_addresses, insert=True)
    bulk_insert_users(users_data_and_customer_group, insert=True)
    bulk_insert_phones(phone_number_and_customer_group, insert=True)
    return "Successfully inserted"



if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0')
