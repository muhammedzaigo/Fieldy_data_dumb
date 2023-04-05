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

ERROR_TARGET_EMAIL = os.getenv('ERROR_TARGET_EMAIL')
if ERROR_TARGET_EMAIL is None:
    ERROR_TARGET_EMAIL = "mrahil7510@gmail.com"
    
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
                msg = Message('Feildy Message', sender=str(os.getenv('MAIL_SENDER')),
                              recipients=[str(ERROR_TARGET_EMAIL)])
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

            import_sheet = import_sheets(file)

            json_format = json.loads(json_format)
            currect_json_map_and_which_user_type_check = currect_json_map_and_which_user_type(
                json_format)
            json_format = currect_json_map_and_which_user_type_check["json_format"]
            which_user = currect_json_map_and_which_user_type_check["user_type"]

            remove_duplicates_sheet = remove_duplicates_in_sheet(
                import_sheet["import_sheet"], which_user)
            cleaned_data = remove_duplicates_sheet["cleaned_data"]
            duplicate_data = remove_duplicates_sheet["removed_rows"]

            json_count = len(json_format.keys())
            field_names = remove_duplicates_sheet["fieldnames"]
            field_names = field_names[0:json_count]

            context = {
                "TENANT_ID": TENANT_ID,
                "which_user": which_user,
                "created_by": created_by,
                "filename": file.filename
            }

            organizationed_and_skip_sheet_data = organizing_all_sheets_using_json_format(
                context, cleaned_data, field_names, json_format, duplicate_data, target_email)

            data_count_context = organizationed_and_skip_sheet_data["data_count_context"]

            if which_user == ORGAZANAIZATION:
                if "remove_dupicate_name_dict" in remove_duplicates_sheet.keys():
                    remove_dupicate_name_dict = remove_duplicates_sheet["remove_dupicate_name_dict"]

                    if organizationed_and_skip_sheet_data["success"]:
                        if len(remove_dupicate_name_dict) != 0:

                            user_first_name = False
                            for key, value in json_format.items():
                                if value["parent"] == "users" and value["table_slug"] == "first_name":
                                    user_first_name = True
                                    break

                            if user_first_name:
                                context.update({"dupicate_name_in_csv": True})
                                organizationed_and_skip_sheet_data = organizing_all_sheets_using_json_format(
                                    context, remove_dupicate_name_dict, field_names, json_format, [], None)
                            else:
                                send_mail_skip_data_and_invalid_data_convert_to_csv(
                                    field_names, [], [], remove_dupicate_name_dict, target_email)

            delete_csv_file(import_sheet)
            response = {
                'message': 'File imported successfully',
                "data_count_context": data_count_context,
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


def organizing_all_sheets_using_json_format(context, cleaned_data, field_names, json_format, duplicate_data, target_email):
    retrive_customer_data = get_bulk_retrive_using_tenant_id(
        context, json_format)
    print(retrive_customer_data)
    organized_customer_list = []
    customer_list = []
    invalid_data = []
    skip_data = []
    same_organization_diffrent_user = []

    for row_index, line in enumerate(cleaned_data, 1):

        field_type = divide_to_field_type_with_json_format(
            row_index, line, field_names, json_format, context, retrive_customer_data)

        if len(field_type["organized_customer_list"]) != 0:
            organized_customer_list.append(
                field_type["organized_customer_list"])

        if len(field_type["customer_list"]) != 0:
            customer_list.append(field_type["customer_list"])

        if len(field_type["invalid_data"]) != 0:
            invalid_data.append(field_type["invalid_data"])

        if len(field_type["skip_data"]) != 0:
            skip_data.append(field_type["skip_data"])

        if field_type["same_organization_diffrent_user"]:
            same_organization_diffrent_user.append(
                field_type["same_organization_diffrent_user"])
    
    tables_name = get_table_names_in_json_condition(json_format)
    bulk_insert_id = get_bulk_insert_id(context,insert=True)
    context.update({'bulk_insert_id': bulk_insert_id})
    organized_customer_list = table_name_use_suparat_all_data(
        organized_customer_list, tables_name)

    if context["which_user"] == CONTACT:
        context.update({"custemer_type": "contact_customer"})
        bulk_insert_using_bulk_type(
            context, organized_customer_list, customer_list)

    if context["which_user"] == ORGAZANAIZATION:
        context.update({"custemer_type": "company_customer"})
        context.update(
            {"same_organization_diffrent_user": same_organization_diffrent_user})
        bulk_insert_using_bulk_type(
            context, organized_customer_list, customer_list)
    if target_email:
        send_mail = threading.Thread(target=send_mail_skip_data_and_invalid_data_convert_to_csv, args=(
            field_names, skip_data, invalid_data, duplicate_data, target_email))
        send_mail.start()

    success_count = len(organized_customer_list["customer_group"])
    if len(same_organization_diffrent_user) != 0:
        success_count += len(same_organization_diffrent_user)
    data_count_context = {
        "invalid_data": len(invalid_data),
        "duplicate_data": len(duplicate_data),
        "skip_data": len(skip_data),
        "success_count": success_count,
    }
    return {
        "data_count_context": data_count_context,
        "success": True,
    }


def divide_to_field_type_with_json_format(row_index, line, field_names, json_format, context, retrive_customer_data):

    customer_list = []
    organized_customer_list = []
    invalid_data = []
    skip_data = []
    same_organization_diffrent_user = None
    json_format_keys = json_format.keys()
    invalid = False
    for column_index, key in enumerate(json_format_keys):

        user_type = json_format[key]['entity']
        table_name = json_format[key]['parent']
        column_name = json_format[key]['table_slug']
        validation = json_format[key]['validation']
        field_type = json_format[key]['field_type']

        try:
            field_name = field_names[column_index]
            value = line[field_name]
        except:
            pass
        if value == ".":
            value = ""

        if user_type == "contact":
            field_format_return_dict = finding_which_data(row_index,
                                                          user_type, table_name, column_name, validation, field_type, value, column_index)
            if field_format_return_dict["valid"]:
                customer_list.append((field_format_return_dict))
                invalid_data.append((field_format_return_dict))
            else:
                invalid = True
                invalid_data.append((field_format_return_dict))

                field_format_return_dict_copy = field_format_return_dict.copy()
                field_format_return_dict_copy["value"] = ""
                customer_list.append((field_format_return_dict_copy))

        if user_type == "organization":
            field_format_return_dict = finding_which_data(row_index,
                                                          user_type, table_name, column_name, validation, field_type, value, column_index)
            if field_format_return_dict["valid"]:
                customer_list.append((field_format_return_dict))
                invalid_data.append((field_format_return_dict))
            else:
                invalid = True
                invalid_data.append((field_format_return_dict))
                
                field_format_return_dict_copy = field_format_return_dict.copy()
                field_format_return_dict_copy["value"] = ""
                customer_list.append((field_format_return_dict_copy))

    if not invalid:
        invalid_data = []
                
    customer_list = add_new_field_based_on_user_type(
        row_index, customer_list, context["which_user"])

    if context["which_user"] == CONTACT:
        skip = is_skip_data(row_index,context, customer_list, retrive_customer_data)
        customer_list = skip["customer_list"]
        if skip["skip"]:
            skip_data = customer_list
        else:
            organized_customer_list = organizing_with_table_name(
                row_index, customer_list)

    if context["which_user"] == ORGAZANAIZATION:
        skip = is_skip_data(row_index,context, customer_list,
                            retrive_customer_data, organization=True)
        customer_list = skip["customer_list"]
        if skip["skip"]:
            skip_data = customer_list
            if "same_organization_name_diffrent_user" in skip.keys():
                same_organization_diffrent_user = skip["same_organization_name_diffrent_user"]
        else:
            organized_customer_list = organizing_with_table_name(
                row_index, customer_list)

    contaxt = {
        "organized_customer_list": organized_customer_list,
        "customer_list": customer_list,
        "invalid_data": invalid_data,
        "skip_data": skip_data,
        "same_organization_diffrent_user": same_organization_diffrent_user
    }
    return contaxt


def is_skip_data(row_index,context, customer_list, retrive_customer_data, organization=False):
    if organization:
        context_data = skip_organization(
            row_index,context, customer_list, retrive_customer_data)
    else:
        context_data = skip_contact(row_index,customer_list, retrive_customer_data)
    return context_data


def skip_organization(row_index,context, customer_list, retrive_customer_data):
    same_organization_name_diffrent_user = ["", "", "", "", "", "", ""]
    organization_user = False
    skip = False
    name = False
    not_branch_addresses = False
    not_address = False
    remove_customer_list_is_delete_true = []

    for customer in customer_list:
        if customer["column_name"] == "name" and customer["table_name"] == "customer_group":
            name = len(str(customer["value"])) != 0

        if customer["column_name"] == "line_1" and customer["table_name"] == "addresses":
            if len(str(customer["value"])) == 0:
                not_address = True
                customer["is_deleted"] = True
            else:
                customer_list.append(add_new_field(
                    "organization", "addresses", "row_index",row_index, row_index))

        if customer["column_name"] == "line_1" and customer["table_name"] == "branch_addresses":
            if len(str(customer["value"])) == 0:
                not_branch_addresses = True
                customer["is_deleted"] = True
            else:
                customer_list.append(add_new_field(
                    "organization", "branch_addresses", "row_index",row_index, row_index))
                
        if customer["column_name"] == "number" and customer["table_name"] == "phones":
            if len(str(customer["value"])) == 0:
                customer["is_deleted"] = True

        if not_address:
            if customer["table_name"] == "addresses":
                customer["is_deleted"] = True

        if not_branch_addresses:
            if customer["table_name"] == "branch_addresses":
                customer["is_deleted"] = True

    for customer in customer_list:
        if customer["is_deleted"] != True:
            remove_customer_list_is_delete_true.append(customer)

    if not name:
        skip = True

    if name and not skip:
        for retrive in retrive_customer_data:
            organization_name = False
            organization_first_name = False
            organization_full_name = ""

            for customer in remove_customer_list_is_delete_true:
                if customer["column_name"] == "name":
                    if retrive[1] == customer["value"]:
                        organization_name = True
                        skip = True

                if customer["column_name"] == "first_name" and customer["table_name"] == "users":
                    if organization_name:

                        if "dupicate_name_in_csv" in context.keys():
                            organization_first_name = True
                            organization_user = True
                            same_organization_name_diffrent_user[1] = customer["value"]
                            same_organization_name_diffrent_user[6] = retrive[0]
                            organization_full_name = customer["value"]
                        else:
                            if retrive[27] != customer["value"]:
                                organization_first_name = True
                                organization_user = True
                                same_organization_name_diffrent_user[1] = customer["value"]
                                same_organization_name_diffrent_user[6] = retrive[0]
                                organization_full_name = customer["value"]

                if organization_first_name:
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

    context_val = {}
    if skip:
        if organization_user:
            context_val.update(
                {"same_organization_name_diffrent_user": same_organization_name_diffrent_user})

    context_val.update(
        {"skip": skip, "customer_list": remove_customer_list_is_delete_true})
    return context_val


def skip_contact(row_index,customer_list, retrive_customer_data):
    skip = False
    name = False
    not_address = False
    remove_customer_list_is_delete_true = []
    for customer in customer_list:
        if customer["column_name"] == "first_name" and customer["table_name"] == "customer_group":
            name = len(str(customer["value"])) != 0

        if customer["column_name"] == "line_1" and customer["table_name"] == "branch_addresses":
            if len(str(customer["value"])) == 0:
                not_address = True
                customer["is_deleted"] = True
            else:
                customer_list.append(add_new_field(
                    "organization", "branch_addresses", "row_index",row_index, row_index))
                
        if customer["column_name"] == "number" and customer["table_name"] == "phones":
            if len(str(customer["value"])) == 0:
                customer["is_deleted"] = True

        if not_address:
            if customer["table_name"] == "branch_addresses":
                customer["is_deleted"] = True

    for customer in customer_list:
        if customer["is_deleted"] != True:
            remove_customer_list_is_delete_true.append(customer)

    if not name:
        skip = True
    if name and not skip:
        for retrive in retrive_customer_data:
            first_name = False
            last_name = False
            email = False
            for customer in remove_customer_list_is_delete_true:
                if customer["column_name"] == "first_name":
                    if retrive[29] == customer["value"]:
                        first_name = True
                if customer["column_name"] == "last_name":
                    if first_name:
                        if retrive[30] == customer["value"]:
                            last_name = True
                if customer["column_name"] == "email":
                        if retrive[2] == customer["value"]:
                            email = True
            if email:
                skip = True
                break
            if first_name and last_name and email:
                skip = True
                break
    return {"skip": skip, "customer_list": remove_customer_list_is_delete_true}


def add_new_field_based_on_user_type(row_index, customer_list, which_user):
    if which_user == CONTACT:
        customer_list = add_new_field_in_contact(customer_list, row_index)

    if which_user == ORGAZANAIZATION:
        customer_list = add_new_field_in_organization(customer_list, row_index)

    return customer_list


def add_new_field_in_contact(contact_customer_list, row_index):
    contact_customer_list.append(add_new_field(
        "contact", "customer_group", "customer_type", "contact_customer", row_index))
    contact_customer_list = common_fields(
        contact_customer_list, "contact", row_index)
    return contact_customer_list


def add_new_field_in_organization(organization_customer_list, row_index):
    organization_customer_list.append(add_new_field(
        "organization", "customer_group", "customer_type", "company_customer", row_index))
    organization_customer_list = common_fields(
        organization_customer_list, "organization", row_index)
    return organization_customer_list


def common_fields(customer_list, user_type, row_index):
    customer_list.append(add_new_field(
        user_type, "customer_group", "type", "customer_company", row_index))
    customer_list.append(add_new_field(
        user_type, "customer_group", "status", 5, row_index))  
    customer_list.append(add_new_field(
        user_type, "customer_group", "row_index", row_index, row_index))  
    return customer_list


def add_new_field(user_type, table_name, column_name, value, row_index):
    field_format_dict = {}
    field_format_dict.update(
        {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value, "valid": True, "is_deleted": False, "customer": False, "line_number": row_index})
    return field_format_dict


def finding_which_data(row_index, user_type, table_name, column_name, validation, field_type, value, column_index):
    field_format_dict = {}
    if len(str(value)) != 0 or pd.isna(value):
        valid = {"valid": False, "message": ""}
        valid = check_validation(valid, validation, field_type, value)
    else:
        valid = {"valid": True}
    if valid["valid"]:
        field_format_dict.update(
            {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value, "valid": valid["valid"], "is_deleted": False, "line_number": row_index, "column_number": column_index})
    else:
        message = valid["message"]
        field_format_dict.update(
            {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": f"{value} - {message} ", "valid": valid["valid"], "is_deleted": False, "line_number": row_index, "column_number": column_index})
    return field_format_dict


def check_validation(valid, validation, field_type, value):
    min = int(validation["min"]) if len(validation["min"]) != 0 else 1
    max = int(validation["max"]) if len(validation["max"]) != 0 else 256
    value = "" if pd.isna(value) else value
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


def organizing_with_table_name(row_index, field_format_return_dict):
    responce_list = []
    responce_dict = {}
    if len(field_format_return_dict) != 0:
        for field_name in field_format_return_dict:
            if field_name['table_name'] not in responce_dict:
                responce_dict[field_name['table_name']] = []
            responce_dict[field_name['table_name']].append(field_name)
        responce_dict = split_customer_group_for_user(
            responce_dict, row_index)
        responce_list.append(responce_dict)
    return responce_list


def split_customer_group_for_user(responce_dict, row_index):
    if "users" not in responce_dict.keys():
        responce_dict["users"] = []

    if "customer_group" in responce_dict.keys():
        user_table_name_dict = {"user_type": "", 'table_name': "customer_group",
                                'column_name': 'name', "value": "", "valid": True, "line_number": row_index}
        contact_person_name = {"user_type": "", 'table_name': "customer_group",
                               'column_name': 'contact_person_name', "value": "", "valid": True, "line_number": row_index}
        contact_person_first_name = {"user_type": "", 'table_name': "customer_group",
                                     'column_name': 'contact_person_first_name', "value": "", "valid": True, "line_number": row_index}
        contact_person_last_name = {"user_type": "", 'table_name': "customer_group",
                                    'column_name': 'contact_person_last_name', "value": "", "valid": True, "line_number": row_index}

        for item in responce_dict["customer_group"]:
            if item["user_type"] == "contact":
                if item['column_name'] == "first_name":
                    responce_dict["users"].append(item)
                    user_table_name_dict["value"] = item["value"]
                    contact_person_first_name["value"] = item["value"]
                    contact_person_name["value"] = item["value"]

                if item['column_name'] == "last_name":
                    responce_dict["users"].append(item)
                    contact_person_last_name["value"] = item["value"]
                    user_table_name_dict["value"] += ' '+item["value"]
                    contact_person_name["value"] += " "+item["value"]

                if item['column_name'] == "email":
                    responce_dict["users"].append(item)
                if item['column_name'] == "job_title":
                    responce_dict["users"].append(item)

                user_table_name_dict["user_type"] = item['user_type']
                contact_person_name["user_type"] = item['user_type']
                contact_person_first_name["user_type"] = item['user_type']
                contact_person_last_name["user_type"] = item['user_type']

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
            responce_dict["customer_group"].append(contact_person_name)
            responce_dict["customer_group"].append(contact_person_first_name)
            responce_dict["customer_group"].append(contact_person_last_name)

        responce_dict["customer_group"] = [d for d in responce_dict["customer_group"]
                                           if d['column_name'] not in ('first_name', 'last_name')]
        # add name field in users dictionary
        responce_dict["users"].append(user_table_name_dict)
    return responce_dict
# --------------------------------step 2 --------------------------------


def send_mail_skip_data_and_invalid_data_convert_to_csv(field_names, skip_data, invalid_data, duplicate_data, target_email):
    field_names_copy = field_names.copy()
    field_names_copy.insert(0, "line Number")
    if len(skip_data) != 0:
        skip_data_count = len(skip_data)
        send_mail_skip_data = threading.Thread(
            target=send_skipped_data, args=(field_names_copy, skip_data, target_email, skip_data_count))
        send_mail_skip_data.start()
    if len(invalid_data) != 0:        
        invalid_data_count = len(invalid_data)
        send_mail_invalid_data = threading.Thread(
            target=send_invalid_data, args=(field_names_copy, invalid_data, target_email, invalid_data_count))
        send_mail_invalid_data.start()
    if len(duplicate_data) != 0:
        duplicate_data_count = len(duplicate_data)
        send_mail_duplicate_data = threading.Thread(
            target=send_duplicate_data, args=(field_names_copy, duplicate_data, target_email, duplicate_data_count))
        send_mail_duplicate_data.start()
    return


def send_skipped_data(field_names, skip_data, target_email, skip_data_count):
    all_data_list = []
    for data in skip_data:
        single_line_data = [""] * len(field_names)
        line_number = 0
        for value in data:
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
        for value in data:
            if "column_number" in value.keys():
                single_line_data[value["column_number"]+1] = value["value"]
            line_number = value["line_number"]
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
    try:
        for key, datas in duplicate_data.items():
            datas.update({"line Number": key+2})
        df = pd.DataFrame.from_dict(
            duplicate_data, orient='index', columns=field_names)
    except:
        for index, datas in enumerate(duplicate_data):
            datas.update({"line Number": index+2})
        df = pd.DataFrame(duplicate_data, columns=field_names)
    csv_name = f"duplicate_data_"+datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        df.to_csv(f'invalid_data_sheets/{csv_name}.csv', index=False)
    except Exception as e:
        print("Error", str(e))
    send_email(count=duplicate_data_count, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv")

# --------------------------------step 3  --------------------------------


def table_name_use_suparat_all_data(organized_customer_list, tables_names):
    table_name_dict_of_list = all_table_names_convert_dict_of_list(
        tables_names)
    for list_of_dict_items in organized_customer_list:
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


def bulk_insert_using_bulk_type(context, organized_customer_list=[], customer_list=[]):
    customer_group_addresess_list = bulk_insert_function(
        organized_customer_list, context)
    thread = threading.Thread(target=users_and_phones_and_customer_group_addresess_mapping, args=(
        customer_list, customer_group_addresess_list, context))
    thread.start()
    return "Successfully"


def bulk_insert_function(organized_customer_list, context):
    TENANT_ID = context["TENANT_ID"]
    bulk_insert_id = context["bulk_insert_id"]
    created_by = context["created_by"]
    custemer_type = context["custemer_type"]

    retrive_customer_group_data_use_bulk_insert_id = []
    retrive_addresses_data_use_bulk_insert_id = []

    for table_name, all_values in organized_customer_list.items():
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
        # create_avatar_then_dumb_files_db_and_map_customer_group.start()

    return {
        "retrive_customer_group": retrive_customer_group_data_use_bulk_insert_id,
        "retrive_addresses": retrive_addresses_data_use_bulk_insert_id
    }


def create_avatar_then_dumb_files_db_and_map_customer_group_thread(customer_group_id_and_emails: tuple = (), bulk_insert_id: int = 1, TENANT_ID=0):
    try:
        files_db_dump_data = []
        files_identifier_list = []
        create_avatar_names = []

        for index, fields in enumerate(customer_group_id_and_emails, 1):
            name = f"{fields[2]}_{index}"
            file_name = f"{name}__{TENANT_ID}_{bulk_insert_id}_" + \
                datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name_with_ext = f"{file_name}.png"

            create_avatar_names.append((name, file_name_with_ext))
            files_db_dump_data.append((TENANT_ID, MIME, file_name_with_ext,
                                       file_name_with_ext, datetime.datetime.now(), bulk_insert_id))
            # file_identifier and customer_group_id
            files_identifier_list.append((file_name_with_ext, fields[0]))

        create_avatar_thread = threading.Thread(
            target=create_avatar, args=(create_avatar_names, True,))
        create_avatar_thread.start()

        bulk_insert_files_list = bulk_insert_files(
            files_db_dump_data, bulk_insert_id, insert=True, select=True)

        update_file_id_custemer_group = []

        for file in bulk_insert_files_list:
            file_id = file[0]
            file_identifier = file[1]

            for file_items in files_identifier_list:
                if file_items[0] == file_identifier:
                    update_file_id_custemer_group.append(
                        (file_items[1], file_id, TENANT_ID,bulk_insert_id, datetime.datetime.now()))
                    continue        
        bulk_update_customer_group(update_file_id_custemer_group, insert=True)
        return "File Upload Successfully"
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

def users_and_phones_and_customer_group_addresess_mapping(row_ways_customer_list, customer_group_addresess_list, context):
    try:
        TENANT_ID = context["TENANT_ID"]
        created_by = context["created_by"]
        which_user = context["which_user"]
        bulk_insert_id = context["bulk_insert_id"]

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
            customer_website = None
            customer_row_index = None
            
            phone = None
            
            addresses_line_1 = None
            addresses_line_2 = None
            addresses_city = None
            addresses_city = None
            addresses_state = None
            addresses_zip_code  = None
            addresses_row_index = None
            
            branch_addresses_line_1 = None
            branch_addresses_line_2 = None
            branch_addresses_first_name  = None
            branch_addresses_last_name = None
            branch_addresses_branch_name = None
            branch_addresses_city = None
            branch_addresses_state = None
            branch_addresses_zip_code = None
            branch_addresses_row_index = None
            
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
                    
                if value["column_name"] == "website" and value["table_name"] == "customer_group":
                    customer_website = value["value"]

                if value["column_name"] == "row_index" and value["table_name"] == "customer_group":
                    customer_row_index = value["value"]


                if value["column_name"] == "line_1" and value["table_name"] == "addresses":
                    addresses_line_1 = value["value"]

                if value["column_name"] == "line_2" and value["table_name"] == "addresses":
                    addresses_line_2 = value["value"]

                if value["column_name"] == "city" and value["table_name"] == "addresses":
                    addresses_city = value["value"]
                
                if value["column_name"] == "state" and value["table_name"] == "addresses":
                    addresses_state = value["value"]    
                    
                if value["column_name"] == "zip_code" and value["table_name"] == "addresses":
                    addresses_zip_code = value["value"]
                    
                if value["column_name"] == "row_index" and value["table_name"] == "addresses":
                    addresses_row_index = value["value"]
                    
                    

                if value["column_name"] == "line_1" and value["table_name"] == "branch_addresses":
                    branch_addresses_line_1 = value["value"]

                if value["column_name"] == "line_2" and value["table_name"] == "branch_addresses":
                    branch_addresses_line_2 = value["value"]

                if value["column_name"] == "first_name" and value["table_name"] == "branch_addresses":
                    branch_addresses_first_name = value["value"]

                if value["column_name"] == "last_name" and value["table_name"] == "branch_addresses":
                    branch_addresses_last_name = value["value"]
                    
                if value["column_name"] == "branch_name" and value["table_name"] == "branch_addresses":
                    branch_addresses_branch_name = value["value"]

                if value["column_name"] == "city" and value["table_name"] == "branch_addresses":
                    branch_addresses_city = value["value"]
                
                if value["column_name"] == "state" and value["table_name"] == "branch_addresses":
                    branch_addresses_state = value["value"]    
                    
                if value["column_name"] == "zip_code" and value["table_name"] == "branch_addresses":
                    branch_addresses_zip_code = value["value"]
                            
                if value["column_name"] == "row_index" and value["table_name"] == "branch_addresses":
                    branch_addresses_row_index = value["value"]            
                    
                    
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

                    if customer_email == customer_group_id_and_email[1] and (customer_name).strip() == customer_group_id_and_email[2] :
                        if customer_row_index == customer_group_id_and_email[4] and customer_website == customer_group_id_and_email[3]:
                        # map customer_group_pk and phone number for phones table
                            if phone:
                                if len(str(phone)) != 0:
                                    converted_number = re.sub(r'[^0-9]', '', str(phone))
                                    phone_number_and_customer_group.append(
                                        (phone,"work",customer_group_id_and_email[0], TENANT_ID, "App\Model\Tenant\CustomerGroup",converted_number, datetime.datetime.now()))

                            # map customer_group_pk and first name and last name for users table
                            if which_user == ORGAZANAIZATION:
                                if users_first_name != None or users_last_name != None:
                                    users_name = f"{users_first_name} {users_last_name}"
                                    users_data_and_customer_group.append(
                                        (users_name, users_first_name, users_last_name, users_email, users_phone, users_job_title, customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))
                                users_data_and_customer_group.append(
                                    (customer_name, "", "", customer_email, "", "", customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))

                            if which_user == CONTACT:
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
                    address_id = False
                    is_primary = 1

                    for address_id_and_line in address_id_and_lines:
                        if str(addresses_line_1) == address_id_and_line[1] and str(addresses_line_2) == address_id_and_line[2]:
                            if addresses_city == address_id_and_line[4] and addresses_state ==address_id_and_line[5] and addresses_zip_code == address_id_and_line[3]:
                                if addresses_row_index == address_id_and_line[9]:
                                    customer_group_pk_and_address_pk.append(
                                        address_id_and_line[0])
                                    address_id = True
                                    break

                    if address_id:
                        customer_group_pk_and_address_pk.insert(0, TENANT_ID)
                        customer_group_pk_and_address_pk.insert(
                            3, str(is_primary))
                        customer_group_pk_and_address_pk.insert(
                            4, datetime.datetime.now())
                        customer_group_pk_and_address_pk.insert(
                            5, bulk_insert_id)
                        customer_group_addresses.append(
                            (customer_group_pk_and_address_pk))

                if branch_addresses_line_1 is not None:
                    branch_address_id = False
                    if addresses_line_1 is not None:
                        is_primary = 0
                    else:
                        is_primary = 1

                    for address_id_and_line in address_id_and_lines:
                        if str(branch_addresses_line_1) == address_id_and_line[1] and str(branch_addresses_line_2) == address_id_and_line[2]:
                            if branch_addresses_city == address_id_and_line[4] and branch_addresses_state == address_id_and_line[5] and branch_addresses_zip_code == address_id_and_line[3]:
                                if branch_addresses_first_name == address_id_and_line[7] and branch_addresses_last_name == address_id_and_line[8] and branch_addresses_branch_name == address_id_and_line[6] :
                                    if branch_addresses_row_index == address_id_and_line[9]:
                                        customer_group_pk_and_address_pk_branch_address.append(
                                            address_id_and_line[0])
                                        branch_address_id = True
                                        break

                    if branch_address_id:
                        customer_group_pk_and_address_pk_branch_address.insert(
                            0, TENANT_ID)
                        customer_group_pk_and_address_pk_branch_address.insert(
                            3, str(is_primary))
                        customer_group_pk_and_address_pk_branch_address.insert(
                            4, datetime.datetime.now())
                        customer_group_pk_and_address_pk_branch_address.insert(
                            5, bulk_insert_id)
                        customer_group_addresses.append(
                            (customer_group_pk_and_address_pk_branch_address))

        if which_user == ORGAZANAIZATION:
            users_data_and_customer_group = same_organization_diffrent_user(
                users_data_and_customer_group, context, role_id, status, hash_password)

        if len(users_data_and_customer_group) != 0:
            bulk_insert_users(users_data_and_customer_group,0,
                              TENANT_ID, insert=True)

        if len(phone_number_and_customer_group) != 0:
            bulk_insert_phones(phone_number_and_customer_group, insert=True)

        if len(customer_group_addresses) != 0:
            customer_group_addresses = bulk_insert_customer_group_addresses(
                customer_group_addresses,bulk_insert_id, insert=True,select=True)
            if which_user == ORGAZANAIZATION:
                customer_group_using_primary_address = []
                for i in customer_group_addresses:
                    i = list(i)
                    i.append(TENANT_ID)
                    customer_group_using_primary_address.append(i)
                bulk_update_customer_group_using_primary_address(customer_group_using_primary_address, insert=True)

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


def same_organization_diffrent_user(users_data_and_customer_group, context, role_id, status, hash_password):
    TENANT_ID = context["TENANT_ID"]
    created_by = context["created_by"]

    if "same_organization_diffrent_user" in context.keys():
        same_organization_diffrent_user = context["same_organization_diffrent_user"]
        if len(same_organization_diffrent_user) != 0:
            coustomer_id_is_dict = {}
            for i in same_organization_diffrent_user:
                if i[6] not in coustomer_id_is_dict:
                    coustomer_id_is_dict[i[6]] = []
                coustomer_id_is_dict[i[6]].append(i)
            for coustomer_id,values in coustomer_id_is_dict.items():
                retrive_user = bulk_insert_users([],coustomer_id, TENANT_ID, select=True) 
                for i in values:
                    first_name = False
                    last_name = False
                    id = False
                    if len(retrive_user) != 0:
                        for user in retrive_user:
                            if i[1] == user[2]:
                                first_name = True
                            if i[2] == user[3]:
                                last_name = True
                            if i[6] == user[29]:
                                id = True
                            if first_name and last_name and id:
                                break
                    if not first_name and not last_name and id:
                        i.append(TENANT_ID)
                        i.append(role_id)
                        i.append(created_by)
                        i.append(status)
                        i.append(hash_password)
                        i.append(datetime.datetime.now())
                        users_data_and_customer_group.append(tuple(i))
                        
    return users_data_and_customer_group


if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0')
