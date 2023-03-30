from utils.utils import *
from utils.query import *
from dotenv import load_dotenv
import pandas as pd
from flask import *
import datetime
import threading
# from api.xlsx_to_csv import xlsx_convert_csv
# from api.bulk_insert import bulk_insert
# app.register_blueprint(xlsx_convert_csv, url_prefix='/api')
# app.register_blueprint(bulk_insert, url_prefix='/api')
from flask_mail import Mail, Message
from template.email import email_template, error_template
import chardet
import traceback
from flask_cors import CORS

app = Flask(__name__, instance_relative_config=True)
CORS(app)
load_dotenv()

app.secret_key = str(os.getenv('SECRET_KEY'))
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USERNAME'] = str(os.getenv('MAIL_USERNAME'))
app.config['MAIL_PASSWORD'] = str(os.getenv('MAIL_PASSWORD'))
ERROR_TARGET_EMAIL = str(os.getenv('ERROR_TARGET_EMAIL'))

mail = Mail(app)


def send_email(count, file_url, logo_url, target_email, filename=None):
    with app.app_context():
        try:
            msg = Message('Feildy Message', sender=str(os.getenv('MAIL_SENDER')),
                          recipients=[target_email])
            with app.open_resource(file_url) as csv_file:
                msg.attach(filename=filename,
                           content_type="text/csv", data=csv_file.read())
            msg.html = email_template(count=count, logo_url=logo_url)
            mail.send(msg)
            return 'Email sent!'
        except Exception as e:
            return f" email : {str(e)}"


def send_error_thread(message, traceback, logo_url):
    def send_error_email(message, traceback, logo_url):
        with app.app_context():
            try:
                if not ERROR_TARGET_EMAIL:
                    ERROR_TARGET_EMAIL = "muhammed@zaigoinfotech.com"
                msg = Message('Feildy Message', sender=str(os.getenv('MAIL_SENDER')),
                              recipients=[ERROR_TARGET_EMAIL])
                msg.html = error_template(
                    message=message, traceback=traceback, logo_url=logo_url)
                mail.send(msg)
                return 'Email sent!'
            except Exception as e:
                return f" email : {str(e)}"
    send_mail = threading.Thread(
        target=send_error_email, args=(message, traceback, logo_url))
    send_mail.start()
    return " Send error email"


@app.route("/api/bulk_import", methods=['POST'])
def bulk_import_api():
    if request.method == 'POST':
        try:
            if 'file' not in request.files:
                return make_response(jsonify({'message': 'No file uploaded'}), 400)
            file = request.files['file']
            TENANT_ID = request.form.get('tenant_id', None)
            json_format = request.form.get('json_format', None)
            target_email = request.form.get('target_email', None)
            created_by = request.form.get('created_by', None)
            if TENANT_ID == None or json_format == None or created_by == None:
                return make_response(jsonify({'message': 'tenant_id, json_format, created_by is required fields'}), 400)
            import_sheet = file.read()
            file_encoding = chardet.detect(import_sheet)['encoding']
            import_sheet = import_sheet.decode(file_encoding)
            remove_duplicates_sheet = remove_duplicates_in_sheet(import_sheet)
            cleaned_data = remove_duplicates_sheet["cleaned_data"]
            duplicate_data = remove_duplicates_sheet["removed_rows"]
            json_format = json.loads(json_format)
            currect_json_map_and_which_user_type_check = currect_json_map_and_which_user_type(
                json_format)
            json_format = currect_json_map_and_which_user_type_check["json_format"]
            which_user = currect_json_map_and_which_user_type_check["user_type"]
            json_count = len(json_format.keys())
            field_names = remove_duplicates_sheet["fieldnames"]
            splite_field_name_with_json_count = field_names[0:json_count]
            retrive_customer_data = get_bulk_retrive_using_tenant_id(
                TENANT_ID, json_format)
            contact_customer_list = []
            organization_customer_list = []
            row_ways_contact_list = []
            row_ways_organization_list = []
            invalid_data = []
            skip_data = []
            same_organization_diffrent_user = []
            for line_index, line in enumerate(cleaned_data, 1):
                field_type = divide_to_field_type_with_json_format(
                    line_index, line, splite_field_name_with_json_count, json_format, which_user, retrive_customer_data)
                if len(field_type["contact"]) != 0:
                    contact_customer_list.append(field_type["contact"])
                if len(field_type["organization"]) != 0:
                    organization_customer_list.append(
                        field_type["organization"])
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
                if field_type["same_organization_diffrent_user"]:
                    same_organization_diffrent_user.append(
                        field_type["same_organization_diffrent_user"])
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
                context.update(
                    {"same_organization_diffrent_user": same_organization_diffrent_user})

                bulk_insert_using_bulk_type(
                    context, organization_customer_list, row_ways_organization_list)
            if target_email:
                send_mail = threading.Thread(target=send_mail_skip_data_and_invalid_data_convert_to_csv, args=(
                    splite_field_name_with_json_count, skip_data, invalid_data, duplicate_data, target_email))
                send_mail.start()
            response = {
                'message': 'File imported successfully',
            }
            response = make_response(jsonify(response), 200)
            response.headers["Content-Type"] = "application/json"
            return response
        except Exception as e:
            response = {
                'message': 'File imported  Failed',
                "error": {
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }
            }
            send_error_thread(message=response["error"]["message"], traceback=response["error"]
                              ["traceback"], logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp")
            response = make_response(jsonify(response), 400)
            response.headers["Content-Type"] = "application/json"
            return response


# -------------------------------- step 1 --------------------------------

def divide_to_field_type_with_json_format(line_index, line, field_names, json_format, which_user, retrive_customer_data):
    contact_customer_list = []
    organization_customer_list = []
    organized_organization_customer_list = []
    organized_contact_customer_list = []
    invalid_data = []
    skip_data = {}
    same_organization_diffrent_user = None

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
        skip = is_skip_data(contact_customer_list, retrive_customer_data)
        if skip["skip"]:
            skip_data.update({"contact": contact_customer_list})
        else:
            organized_contact_customer_list = organizing_with_table_name(
                line_index, contact_customer_list)
    if which_user == ORGAZANAIZATION:
        skip = is_skip_data(organization_customer_list,
                            retrive_customer_data, organization=True)
        if skip["skip"]:
            skip_data.update({"organization": organization_customer_list})
            if "same_organization_name_diffrent_user" in skip.keys():
                same_organization_diffrent_user = skip["same_organization_name_diffrent_user"]
        else:
            organized_organization_customer_list = organizing_with_table_name(
                line_index, organization_customer_list)

    contaxt = {"contact": organized_contact_customer_list,
               "organization": organized_organization_customer_list,
               "row_ways_contact_list": contact_customer_list,
               "row_ways_organization_list": organization_customer_list,
               "invalid_data": invalid_data,
               "skip_data": skip_data,
               "same_organization_diffrent_user": same_organization_diffrent_user
               }
    return contaxt


def is_skip_data(customer_list, retrive_customer_data, organization=False):
    context = {}
    if organization:
        is_skip_organization = skip_organization(
            customer_list, retrive_customer_data)
        skip = is_skip_organization["skip"]
        if "same_organization_name_diffrent_user" in is_skip_organization.keys():
            context.update(
                {"same_organization_name_diffrent_user": is_skip_organization["same_organization_name_diffrent_user"]})
        context.update({"skip": skip})
    else:
        skip = skip_contact(customer_list, retrive_customer_data)
        context.update({"skip": skip})
    return context


def skip_organization(customer_list, retrive_customer_data):
    same_organization_name_diffrent_user = ["", "", "", "", "", "", ""]
    organization_user = False
    skip = False
    name = False
    line_1 = False
    for customer in customer_list:
        if customer["column_name"] == "name" and customer["table_name"] == "customer_group":
            name = len(str(customer["value"])) != 0
        if customer["column_name"] == "line_1":
            line_1 = len(str(customer["value"])) != 0
        if name and line_1:
            break
    if not name or not line_1:
        skip = True
    if name and not skip:  # if name
        for retrive in retrive_customer_data:
            organization_name = False
            organization_first_name = False
            organization_full_name = ""
            for customer in customer_list:
                if customer["column_name"] == "name":
                    if retrive[1] == customer["value"]:  # retrive[1] name
                        organization_name = True
                        skip = True
                if customer["column_name"] == "first_name" and customer["table_name"] == "users":
                    if organization_name:
                        if retrive[27] != customer["value"]:  # retrive[27] first_name
                            organization_first_name = True
                            organization_user = True
                            same_organization_name_diffrent_user[1] = customer["value"]
                            same_organization_name_diffrent_user[6] = retrive[0]
                            organization_full_name = customer["value"]
                if organization_first_name:
                    # retrive[28] last_name
                    if customer["column_name"] == "last_name" and customer["table_name"] == "users":
                        same_organization_name_diffrent_user[2] = customer["value"]
                        organization_full_name = organization_full_name + \
                            " "+customer["value"]
                        same_organization_name_diffrent_user[0] = organization_full_name
                    if customer["column_name"] == "email" and customer["table_name"] == "users":
                        same_organization_name_diffrent_user[3] = customer["value"]
                    if customer["column_name"] == "phone" and customer["table_name"] == "users":
                        same_organization_name_diffrent_user[4] = customer["value"]
                    if customer["column_name"] == "job_title" and customer["table_name"] == "users":
                        same_organization_name_diffrent_user[5] = customer["value"]
            if skip:
                break
    context = {}
    return_value = False
    if skip:
        return_value = True
        if organization_user:
            context.update(
                {"same_organization_name_diffrent_user": same_organization_name_diffrent_user})
    context.update({"skip": return_value})
    return context


def skip_contact(customer_list, retrive_customer_data):
    skip = False
    name = False
    line_1 = False
    for customer in customer_list:
        if customer["column_name"] == "first_name" and customer["table_name"] == "customer_group":
            name = len(str(customer["value"])) != 0
        if customer["column_name"] == "line_1":
            line_1 = len(str(customer["value"])) == 0
        if line_1:
            if customer["column_name"] in ["line_2", "city", "state", "zip_code", "branch_name"]:
                skip = True
    if not name:
        skip = True
    if name and not skip:  # if first_name
        for retrive in retrive_customer_data:
            first_name = False
            last_name = False
            email = False
            number = False
            not_email = False
            not_phone = False
            for customer in customer_list:
                if customer["column_name"] == "first_name":
                    if retrive[27] == customer["value"]:  # retrive[27] first_name
                        first_name = True
                if customer["column_name"] == "last_name":
                    if first_name:
                        if retrive[28] == customer["value"]:  # retrive[28] last_name
                            last_name = True
                if customer["column_name"] == "email":
                    if first_name:
                        if retrive[2] == customer["value"]:  # retrive[2] email
                            email = True
                else:
                    not_email = True
                if customer["column_name"] == "number":
                    if email:
                        if retrive[65] == customer["value"]:  # retrive[65] phone number
                            number = True
                else:
                    not_phone = True

            if first_name and last_name and email and number:
                skip = True
                break
            if first_name and last_name and email and not_phone:
                skip = True
                break
            if first_name and last_name and number and not_email:
                skip = True
                break
            if not_email and not_phone:
                if first_name and last_name:
                    skip = True
                    break
    return skip


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
    customer_list.append(add_new_field(
        user_type, "customer_group", "status", 5, line_index))
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
    value = str(value) if value else ""
    if len(value) != 0:
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


def send_mail_skip_data_and_invalid_data_convert_to_csv(field_names, skip_data, invalid_data, duplicate_data, target_email):
    field_names.insert(0, "line Number")
    if len(skip_data) != 0:
        skip_data_count = len(skip_data)
        send_mail_skip_data = threading.Thread(
            target=send_skipped_data, args=(field_names, skip_data, target_email, skip_data_count))
        send_mail_skip_data.start()
    if len(invalid_data) != 0:
        invalid_data_count = len(invalid_data)
        send_mail_invalid_data = threading.Thread(
            target=send_invalid_data, args=(field_names, invalid_data, target_email, invalid_data_count))
        send_mail_invalid_data.start()
    if len(duplicate_data) != 0:
        duplicate_data_count = len(duplicate_data)
        send_mail_duplicate_data = threading.Thread(
            target=send_duplicate_data, args=(field_names, duplicate_data, target_email, duplicate_data_count))
        send_mail_duplicate_data.start()
    return


def send_skipped_data(field_names, skip_data, target_email, skip_data_count):
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
    send_email(count=skip_data_count, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv")


def send_invalid_data(field_names, invalid_data, target_email, invalid_data_count):
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
    send_email(count=invalid_data_count, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv")


def send_duplicate_data(field_names, duplicate_data, target_email, duplicate_data_count):
    for key, datas in duplicate_data.items():
        datas.update({"line Number": key+2})
    df = pd.DataFrame.from_dict(
        duplicate_data, orient='index', columns=field_names)
    csv_name = f"duplicate_data_"+datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        df.to_csv(f'invalid_data_sheets/{csv_name}.csv', index=False)
    except Exception as e:
        print("Error", str(e))
    send_email(count=duplicate_data_count, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv")

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
    customer_group_addresess_list = bulk_insert_function(
        customer_list, context)
    thread = threading.Thread(target=users_and_phones_and_customer_group_addresess_mapping, args=(
        row_ways_customer_list, customer_group_addresess_list, context))
    thread.start()
    return "Successfully"


def bulk_insert_function(bulk_insert_list, context):
    TENANT_ID = context["TENANT_ID"]
    bulk_insert_id = context["bulk_insert_id"]
    created_by = context["created_by"]
    custemer_type = context["custemer_type"]

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


def users_and_phones_and_customer_group_addresess_mapping(row_ways_customer_list, customer_group_addresess_list, context):
    try:
        TENANT_ID = context["TENANT_ID"]
        created_by = context["created_by"]
        which_user = context["which_user"]

        customer_group_addresses = []
        phone_number_and_customer_group = []
        users_data_and_customer_group = []
        customer_group_id_and_emails = customer_group_addresess_list["retrive_customer_group"]
        address_id_and_lines = customer_group_addresess_list["retrive_addresses"]
        role_id = retrive_role_id(TENANT_ID, select=True)
        hash_password = password_hash(DEFAULT_PASSWORD)
        status = 5
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
            if len(customer_group_id_and_emails) != 0:
                for customer_group_id_and_email in customer_group_id_and_emails:
                    if customer_email == customer_group_id_and_email[1] and customer_name == customer_group_id_and_email[2]:
                        # map customer_group_pk and phone number for phones table
                        if phone:
                            if len(str(phone)) != 0:
                                phone_number_and_customer_group.append(
                                    (phone, customer_group_id_and_email[0], TENANT_ID, datetime.datetime.now()))
                        # map customer_group_pk and first name and last name for users table
                        if which_user == ORGAZANAIZATION:
                            if users_first_name != None or users_last_name != None:
                                users_name = f"{users_first_name} {users_last_name}"
                                users_data_and_customer_group.append(
                                    (users_name, users_first_name, users_last_name, users_email, users_phone, users_job_title, customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))
                            users_data_and_customer_group.append(
                                (customer_name, "", "", customer_email, "", "", customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))
                        else:
                            users_data_and_customer_group.append(
                                (customer_name, customer_first_name, customer_last_name, customer_email, users_phone, users_job_title, customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))

                        if addresses_line_1 is not None:
                            customer_group_pk_and_address_pk.append(
                                customer_group_id_and_email[0])
                        if branch_addresses_line_1 is not None:
                            customer_group_pk_and_address_pk_branch_address.append(
                                customer_group_id_and_email[0])
                        break
            if len(address_id_and_lines) != 0:
                if addresses_line_1 is not None:
                    for address_id_and_line in address_id_and_lines:
                        if str(addresses_line_1) == address_id_and_line[1] and str(addresses_line_2) == address_id_and_line[2]:
                            customer_group_pk_and_address_pk.append(
                                address_id_and_line[0])
                            break
                    customer_group_pk_and_address_pk.insert(0, TENANT_ID)
                    customer_group_pk_and_address_pk.insert(
                        3, datetime.datetime.now())
                    customer_group_addresses.append(
                        (customer_group_pk_and_address_pk))

                if branch_addresses_line_1 is not None:
                    for address_id_and_line in address_id_and_lines:
                        if str(branch_addresses_line_1) == address_id_and_line[1] and str(branch_addresses_line_2) == address_id_and_line[2]:
                            customer_group_pk_and_address_pk_branch_address.append(
                                address_id_and_line[0])
                            break
                    customer_group_pk_and_address_pk_branch_address.insert(
                        0, TENANT_ID)
                    customer_group_pk_and_address_pk_branch_address.insert(
                        3, datetime.datetime.now())
                    customer_group_addresses.append(
                        (customer_group_pk_and_address_pk_branch_address))

        if which_user == ORGAZANAIZATION:
            same_organization_diffrent_user = context["same_organization_diffrent_user"]
            if len(same_organization_diffrent_user) != 0:
                for i in same_organization_diffrent_user:
                    i.append(TENANT_ID)
                    i.append(role_id)
                    i.append(created_by)
                    i.append(status)
                    i.append(hash_password)
                    i.append(datetime.datetime.now())
                    users_data_and_customer_group.append(i)

        if len(customer_group_addresses) != 0:
            bulk_insert_customer_group_addresses(
                customer_group_addresses, insert=True)
        if len(users_data_and_customer_group) != 0:
            bulk_insert_users(users_data_and_customer_group, insert=True)
        if len(phone_number_and_customer_group) != 0:
            bulk_insert_phones(phone_number_and_customer_group, insert=True)
        return "Successfully inserted"
    except Exception as e:
        response = {
            "error": {
                "message": str(e),
                "traceback": traceback.format_exc()
            }
        }
        send_error_thread(message=response["error"]["message"], traceback=response["error"]
                          ["traceback"], logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp")
        print(json.dumps(response))


if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0')
