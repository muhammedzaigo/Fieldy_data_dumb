from utils.utils import *
from utils.query import *
from dotenv import load_dotenv
import pandas as pd
from flask import *
import datetime
import threading
import requests
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
            currect_json_map = currect_json_map_and_which_user_type(
                json_format)
            json_format = currect_json_map["json_format"]
            which_user = currect_json_map["user_type"]

            remove_duplicates = remove_duplicates_in_sheet(
                import_sheet["import_sheet"], which_user, json_format)

            cleaned_data = remove_duplicates["cleaned_data"]
            duplicate_data = remove_duplicates["removed_rows"]
            field_names = remove_duplicates["fieldnames"]

            context = {
                "TENANT_ID": TENANT_ID,
                "which_user": which_user,
                "created_by": created_by,
                "filename": file.filename
            }

            organizationed_data = organizing_sheets_using_json_format(
                context, cleaned_data, field_names, json_format, duplicate_data, target_email)
            data_count_context = organizationed_data["data_count_context"]
            customer_group_addresess_list = organizationed_data["customer_group_addresess_list"]
            customer_list = organizationed_data["customer_list"]
            same_org_organizationed_data = organizationed_data["same_org_diffrent_user"]

            if which_user == ORGAZANAIZATION:
                same_org_organizationed_data = duplicate_organization_name_in_sheet(
                    context, field_names, same_org_organizationed_data, remove_duplicates, organizationed_data, json_format, target_email)

            same_org_organizationed_data_id_dict = same_org_organizationed_data_id_convert_dict(
                same_org_organizationed_data)

            context.update(
                {"same_organization_diffrent_user": same_org_organizationed_data_id_dict["same_org_organizationed_data_id_dict"]})
            data_count_context["success_count"] += same_org_organizationed_data_id_dict["success_count"]
            data_count_context["skip_data"] += same_org_organizationed_data_id_dict["skip_count"]

            bulk_insert_user_and_address(
                context, customer_group_addresess_list, customer_list)

            delete_csv_file(import_sheet)
            api_call_for_cashe(TENANT_ID,customer_group_addresess_list)
            
            customer_group_ids = []
            for id in customer_group_addresess_list["retrive_customer_group"]:
                customer_group_ids.append(id[0])
                
            response = {
                'message': 'File imported successfully',
                "data_count_context": data_count_context,
                "customer_group_ids":customer_group_ids,
                "tenant_id":TENANT_ID
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


def same_org_organizationed_data_id_convert_dict(same_org_organizationed_data):
    same_org_organizationed_data_id_dict = {}
    success_count = 0
    skip_count = 0
    email_list = []
    for i in same_org_organizationed_data:
        if i[6] not in same_org_organizationed_data_id_dict:
            same_org_organizationed_data_id_dict[i[6]] = []
        if i[3] != None:
            if i[3] not in email_list:
                email_list.append(i[3])
                success_count += 1
                same_org_organizationed_data_id_dict[i[6]].append(i)
            else:
                skip_count += 1
        else:
            success_count += 1
            same_org_organizationed_data_id_dict[i[6]].append(i)

    return {"same_org_organizationed_data_id_dict": same_org_organizationed_data_id_dict, "success_count": success_count, "skip_count": skip_count}


def duplicate_organization_name_in_sheet(context, field_names, same_org_organizationed_data, remove_duplicates, organizationed_data, json_format, target_email):
    if "remove_dupicate_name_dict" in remove_duplicates.keys():
        remove_dupicate_name_dict = remove_duplicates["remove_dupicate_name_dict"]

        if organizationed_data["success"]:
            if len(remove_dupicate_name_dict) != 0:

                user_first_name = False
                for key, value in json_format.items():
                    if value["parent"] == "users" and value["table_slug"] == "first_name":
                        user_first_name = True
                        break

                if user_first_name:
                    context.update({"dupicate_name_in_csv": True})
                    duplicate_organizationed_data = organizing_sheets_using_json_format(
                        context, remove_dupicate_name_dict, field_names, json_format, [], None)

                    for i in duplicate_organizationed_data["same_org_diffrent_user"]:
                        same_org_organizationed_data.append(i)
                else:
                    send_mail_skip_data_and_invalid_data_convert_to_csv(
                        field_names, [], [], remove_dupicate_name_dict, target_email)
    return same_org_organizationed_data


def organizing_sheets_using_json_format(context, cleaned_data, field_names, json_format, duplicate_data, target_email):
    retrive_customer_data = get_bulk_retrive_using_tenant_id(
        context, json_format)
    responce_read_data_by_rows = read_data_row_by_rows(
        cleaned_data, field_names, json_format, context, retrive_customer_data)

    organized_customer_list = responce_read_data_by_rows["organized_customer_list"]
    customer_list = responce_read_data_by_rows["customer_list"]
    invalid_data = responce_read_data_by_rows["invalid_data"]
    skip_data = responce_read_data_by_rows["skip_data"]
    same_org_diffrent_user = responce_read_data_by_rows["same_organization_diffrent_user"]

    tables_name = get_table_names_in_json_condition(json_format)
    bulk_insert_id = get_bulk_insert_id(context, insert=True)
    context.update({'bulk_insert_id': bulk_insert_id})
    organized_customer_list = table_name_use_suparat_all_data(
        organized_customer_list, tables_name)

    if context["which_user"] == CONTACT:
        context.update({"custemer_type": "contact_customer"})
        customer_group_addresess_list = bulk_insert_function(
            organized_customer_list, context)

    if context["which_user"] == ORGAZANAIZATION:
        context.update({"custemer_type": "company_customer"})
        customer_group_addresess_list = bulk_insert_function(
            organized_customer_list, context)

    if target_email:
        send_mail = threading.Thread(target=send_mail_skip_data_and_invalid_data_convert_to_csv, args=(
            field_names, skip_data, invalid_data, duplicate_data, target_email))
        send_mail.start()

    data_count_context = {
        "invalid_data": len(invalid_data),
        "duplicate_data": len(duplicate_data),
        "skip_data": len(skip_data),
        "success_count": len(organized_customer_list["customer_group"]),
    }
    return {
        "data_count_context": data_count_context,
        "success": True,
        "customer_group_addresess_list": customer_group_addresess_list,
        "customer_list": customer_list,
        "same_org_diffrent_user": same_org_diffrent_user
    }


def read_data_row_by_rows(cleaned_data, field_names, json_format, context, retrive_customer_data):
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

    if "dupicate_name_in_csv" in context.keys():
        context = {
            "organized_customer_list": [],
            "customer_list": [],
            "invalid_data": [],
            "skip_data": [],
            "same_organization_diffrent_user": same_organization_diffrent_user
        }
    else:
        context = {
            "organized_customer_list": organized_customer_list,
            "customer_list": customer_list,
            "invalid_data": invalid_data,
            "skip_data": skip_data,
            "same_organization_diffrent_user": same_organization_diffrent_user
        }
    return context


def divide_to_field_type_with_json_format(row_index, line, field_names, json_format, context, retrive_customer_data):

    customer_list = []
    organized_customer_list = []
    skip_data = []
    same_organization_diffrent_user = None

    invalid_data = []
    invalid = False

    json_format_keys = json_format.keys()
    for key in json_format_keys:

        user_type = json_format[key]['entity']
        table_name = json_format[key]['parent']
        column_name = json_format[key]['table_slug']
        validation = json_format[key]['validation']
        field_type = json_format[key]['field_type']
        column_index = json_format[key]['sheet_header_index']
        field_name = field_names[column_index]
        value = line[field_name]
        
        if user_type == "contact":
            field_format_return_dict = finding_which_data(row_index, column_index,
                                                          user_type, table_name, column_name, validation, field_type, value)
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
            field_format_return_dict = finding_which_data(row_index, column_index,
                                                          user_type, table_name, column_name, validation, field_type, value)
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
        skip = is_skip_data(row_index, context,
                            customer_list, retrive_customer_data)
        customer_list = skip["customer_list"]
        if skip["skip"]:
            skip_data = customer_list
        else:
            organized_customer_list = organizing_with_table_name(
                row_index, customer_list)

    if context["which_user"] == ORGAZANAIZATION:
        skip = is_skip_data(row_index, context, customer_list,
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


def is_skip_data(row_index, context, customer_list, retrive_customer_data, organization=False):
    if organization:
        context_data = skip_organization(
            row_index, context, customer_list, retrive_customer_data)
    else:
        context_data = skip_contact(
            row_index, customer_list, retrive_customer_data)
    return context_data


def skip_organization(row_index, context, customer_list, retrive_customer_data):
    same_organization_name_diffrent_user = ["", "", "", "", "", "", ""]
    organization_user = False
    skip = False
    is_name = False
    not_branch_addresses = False
    not_address = False
    remove_customer_list_is_delete_true = []
    not_address_fields = True
    not_branch_addresses_fields = True
    message = ""
    organization_user_check_list = [None, None, None, None, None, None, None]
    name = None
    email = None
    for customer in customer_list:
        if customer["column_name"] == "name" and customer["table_name"] == "customer_group":
            if len(str(customer["value"])) != 0:
                is_name = True
                name = str(customer["value"])

        if customer["column_name"] == "first_name" and customer["table_name"] == "users":
            if len(str(customer["value"])) != 0:
                organization_user_check_list[0] = str(customer["value"])
                same_organization_name_diffrent_user[1] = str(
                    customer["value"])

        if customer["column_name"] == "last_name" and customer["table_name"] == "users":
            if len(str(customer["value"])) != 0:
                organization_user_check_list[1] = str(customer["value"])
                same_organization_name_diffrent_user[2] = str(
                    customer["value"])

        if customer["column_name"] == "email" and customer["table_name"] == "users":
            if len(str(customer["value"])) != 0:
                organization_user_check_list[2] = str(customer["value"])
                same_organization_name_diffrent_user[3] = str(
                    customer["value"])
                email = str(customer["value"])

        if customer["column_name"] == "phone" and customer["table_name"] == "users":
            if len(str(customer["value"])) != 0:
                organization_user_check_list[3] = str(customer["value"])
                same_organization_name_diffrent_user[4] = str(
                    customer["value"])

        if customer["column_name"] == "job_title" and customer["table_name"] == "users":
            if len(str(customer["value"])) != 0:
                organization_user_check_list[4] = str(customer["value"])
                same_organization_name_diffrent_user[5] = str(
                    customer["value"])


        if customer["column_name"] == "number" and customer["table_name"] == "phones":
            if len(str(customer["value"])) == 0:
                customer["is_deleted"] = True
            else:
                organization_user_check_list[5] = str(customer["value"])
                converted_number = re.sub(r'[^0-9]', '', str(customer["value"]))
                converted_number = re.sub(r'\D', '', converted_number)
                organization_user_check_list[6] = converted_number

        if customer["table_name"] == "addresses":
            if customer["column_name"] == "line_1":
                not_address_fields = False
                if len(str(customer["value"])) == 0:
                    not_address = True
                    customer["is_deleted"] = True
                else:
                    customer_list.append(add_new_field(
                        "organization", "addresses", "row_index", row_index, row_index))

        if customer["table_name"] == "branch_addresses":
            if customer["column_name"] == "line_1":
                not_branch_addresses_fields = False
                if len(str(customer["value"])) == 0:
                    not_branch_addresses = True
                    customer["is_deleted"] = True
                else:
                    customer_list.append(add_new_field(
                        "organization", "branch_addresses", "row_index", row_index, row_index))


    if not_address_fields or not_address:
        for customer in customer_list:
            if customer["table_name"] == "addresses":
                customer["is_deleted"] = True

    if not_branch_addresses_fields or not_branch_addresses:
        for customer in customer_list:
            if customer["table_name"] == "branch_addresses":
                customer["is_deleted"] = True

    for customer in customer_list:
        if customer["is_deleted"] != True:
            remove_customer_list_is_delete_true.append(customer)

    if not is_name:
        skip = True
        message = "Not given a Organization name"

    if is_name and not skip:
        is_email = False
        organization_name = False
        if name:
            if len(retrive_customer_data["names_List"]) != 0:
                if name in retrive_customer_data["names_List"]:
                    skip = True
                    organization_name = True
                    message = "Already registered Organization name"
                    
        if organization_name:
            if email:
                if len(retrive_customer_data["emails_List"]) != 0:
                    if email in retrive_customer_data["emails_List"]:
                        is_email = True

            if not is_email:
                if len(retrive_customer_data["all_check_List"]) != 0:
                    if organization_user_check_list not in retrive_customer_data["all_check_List"]:
                        organization_user = True

            if "dupicate_name_in_csv" in context.keys():
                organization_user = True

            if organization_user:
                id = retrive_customer_data["name_and_id_dict"][name]
                same_organization_name_diffrent_user[6] = id
                full_name = same_organization_name_diffrent_user[1] + \
                    " "+same_organization_name_diffrent_user[2]
                same_organization_name_diffrent_user[0] = full_name.strip()
                
    context_val = {"customer_list": remove_customer_list_is_delete_true}
    if skip:
        message = add_new_field("organization", "skip", "message", message, row_index)
        customer_list.append(message)
        context_val["customer_list"] = customer_list
        if organization_user:
            context_val.update(
                {"same_organization_name_diffrent_user": same_organization_name_diffrent_user})
    context_val.update({"skip": skip})
    return context_val


def skip_contact(row_index, customer_list, retrive_customer_data):
    skip = False
    name = False
    not_address = False
    not_address_field = True
    remove_customer_list_is_delete_true = []
    email = None
    contact_check_list = [None,None,None,None,None]
    message = ""
    for customer in customer_list:
        if customer["column_name"] == "first_name" and customer["table_name"] == "customer_group":
            if len(str(customer["value"])) != 0:
                contact_check_list[0] =str(customer["value"])
                name = True

        if customer["column_name"] == "last_name" and customer["table_name"] == "customer_group":
            if len(str(customer["value"])) != 0:
                contact_check_list[1] =str(customer["value"])

        if customer["column_name"] == "email" and customer["table_name"] == "customer_group":
            if len(str(customer["value"])) != 0:
                contact_check_list[2] =str(customer["value"])
                email = str(customer["value"])

        if customer["column_name"] == "number" and customer["table_name"] == "phones":
            if len(str(customer["value"])) == 0:
                customer["is_deleted"] = True
            else:
                contact_check_list[3] =str(customer["value"])
                converted_number = re.sub(r'[^0-9]', '', str(customer["value"]))
                converted_number = re.sub(r'\D', '', converted_number)
                contact_check_list[4] = converted_number

        if customer["table_name"] == "branch_addresses":
            if customer["column_name"] == "line_1":
                not_address_field = False
                if len(str(customer["value"])) == 0:
                    not_address = True
                    customer["is_deleted"] = True
                else:
                    customer_list.append(add_new_field(
                        "organization", "branch_addresses", "row_index", row_index, row_index))

    if not_address_field or not_address:
        for customer in customer_list:
            if customer["table_name"] == "branch_addresses":
                customer["is_deleted"] = True

    for customer in customer_list:
        if customer["is_deleted"] != True:
            remove_customer_list_is_delete_true.append(customer)

    if not name:
        skip = True
        message = "Not given a first name"
    if name and not skip:
        if email:
            if email in retrive_customer_data["emails"]:
                skip = True
                message = "Email already exists"
                
        if len(contact_check_list) != 0 and not skip:
            if contact_check_list in retrive_customer_data["return_data_List"]:
                skip = True
                message = "Contact already exists"
    context_val = {"customer_list": remove_customer_list_is_delete_true}
    if skip:
        message = add_new_field("contact", "skip", "message", message, row_index)
        customer_list.append(message)
        context_val["customer_list"] = customer_list
    context_val.update({"skip": skip})
    return context_val


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
        {
            "user_type": user_type,
            "table_name": table_name,
            "column_name": column_name,
            "value": value,
            "valid": True,
            "is_deleted": False,
            "line_number": row_index,
        })
    return field_format_dict


def finding_which_data(row_index, column_index, user_type, table_name, column_name, validation, field_type, value):
    field_format_dict = {}
    valid = check_validation(validation, field_type, value)
    value = valid["value"]
    if valid["valid"]:
        field_format_dict.update(
            {
                "user_type": user_type,
                "table_name": table_name,
                "column_name": column_name,
                "value": valid["value"],
                "valid": valid["valid"],
                "is_deleted": False,
                "line_number": row_index,
                "column_number": column_index
            })
    else:
        message = valid["message"]
        field_format_dict.update(
            {
                "user_type": user_type,
                "table_name": table_name,
                "column_name": column_name,
                "value": f"{value} - {message} ",
                "valid": valid["valid"],
                "is_deleted": False,
                "line_number": row_index,
                "column_number": column_index
            })
    return field_format_dict


def check_validation(validation, field_type, value):
    min = validation["min"]
    max = validation["max"]
    value = valid_value(value)
    if len(value) != 0:
        valid_dict = {"valid": False, "message": ""}
        if field_type == "alpha_numeric":
            valid_dict = is_valid_alphanumeric(value, min, max)
        if field_type == "all_characters":
            valid_dict = is_all_characters(value, min, max)
        if field_type == "website":
            valid_dict = is_valid_url(value, min, max)
        if field_type == "number":
            valid_dict = is_valid_phone_number(value, min, max)
        if field_type == "email":
            valid_dict = is_valid_email(value, min, max)
    else:
        valid_dict = {"valid": True, "message": ""}
    valid_dict.update({"value": value})
    return valid_dict


def valid_value(value):
    value = "" if value == "." else value
    value = "" if pd.isna(value) else value
    value = str(value) 
    value = value if len(value) != 0 else ""
    return value


def organizing_with_table_name(row_index, field_format_return_dict):
    responce_list = []
    responce_dict = {}
    if len(field_format_return_dict) != 0:
        for field_name in field_format_return_dict:
            if field_name['table_name'] not in responce_dict:
                responce_dict[field_name['table_name']] = []
            responce_dict[field_name['table_name']].append(field_name)
        responce_dict = split_and_add_fields_customer_group_for_user(
            responce_dict, row_index)
        responce_list.append(responce_dict)
    return responce_list


def split_and_add_fields_customer_group_for_user(responce_dict, row_index):
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
                    user_table_name_dict["value"] = user_table_name_dict["value"].strip(
                    )

                    contact_person_name["value"] += " "+item["value"]
                    contact_person_name["value"] = contact_person_name["value"].strip(
                    )

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
        massage = None
        for value in data:
            if value["table_name"] == "skip" and value["column_name"] == "message":
                    massage = value["value"]
            if "column_number" in value.keys():
                single_line_data[value["column_number"]+1] = value["value"]
            line_number = value["line_number"]
        single_line_data[0] = line_number
        if massage:
            single_line_data.append(massage)
        all_data_list.append(single_line_data)
    csv_name = f"skipped_data_"+datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        field_names_copy = field_names.copy()
        field_names_copy.append("message")
        df = pd.DataFrame(all_data_list, columns=field_names_copy)
        df.to_csv(f'invalid_data_sheets/{csv_name}.csv', index=False)
    except Exception as e:
        print("Error", str(e))
    send_email(count=skip_data_count, file_url=os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv",massege_type="skipped")


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
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv",massege_type="invalid")


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
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="https://getfieldy.com/wp-content/uploads/2023/01/logo.webp", target_email=target_email, filename=f"{csv_name}.csv",massege_type="duplicate")

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


def bulk_insert_user_and_address(context, customer_group_addresess_list, customer_list=[]):
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
                        (file_items[1], file_id, TENANT_ID, bulk_insert_id, datetime.datetime.now()))
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
            addresses_zip_code = None
            addresses_row_index = None

            branch_addresses_line_1 = None
            branch_addresses_line_2 = None
            branch_addresses_first_name = None
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
                customer_name = customer_name.strip()

            customer_group_pk_and_address_pk = []
            customer_group_pk_and_address_pk_branch_address = []

            if len(customer_group_id_and_emails) != 0:
                for customer_group_id_and_email in customer_group_id_and_emails:

                    if customer_email == customer_group_id_and_email[1] and customer_name == customer_group_id_and_email[2]:
                        if customer_row_index == customer_group_id_and_email[4] and customer_website == customer_group_id_and_email[3]:
                            # map customer_group_pk and phone number for phones table
                            if phone:
                                if len(str(phone)) != 0:
                                    converted_number = re.sub(r'[^0-9]', '', str(phone))
                                    converted_number = re.sub(r'\D', '', converted_number)
                                    phone_number_and_customer_group.append(
                                        (phone, "work", customer_group_id_and_email[0], TENANT_ID, "App\Model\Tenant\CustomerGroup", converted_number, datetime.datetime.now()))

                            # map customer_group_pk and first name and last name for users table
                            if which_user == ORGAZANAIZATION:
                                if users_first_name or users_last_name or users_phone or users_job_title:
                                    users_name = ""
                                    if users_first_name:
                                        users_name = f"{users_first_name} {users_last_name}"
                                        users_name = users_name.strip()
                                    users_data_and_customer_group.append(
                                        (users_name, users_first_name, users_last_name, users_email, users_phone, users_job_title, customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))
                                users_data_and_customer_group.append(
                                    (customer_name, customer_name, "", customer_email, "", "", customer_group_id_and_email[0], TENANT_ID, role_id, created_by, status, hash_password, datetime.datetime.now()))

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
                        if str(addresses_line_1) == address_id_and_line[1] and addresses_line_2 == address_id_and_line[2]:
                            if addresses_city == address_id_and_line[4] and addresses_state == address_id_and_line[5] and addresses_zip_code == address_id_and_line[3]:
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
                        if str(branch_addresses_line_1) == address_id_and_line[1] and branch_addresses_line_2 == address_id_and_line[2]:
                            if branch_addresses_city == address_id_and_line[4] and branch_addresses_state == address_id_and_line[5] and branch_addresses_zip_code == address_id_and_line[3]:
                                if branch_addresses_first_name == address_id_and_line[7] and branch_addresses_last_name == address_id_and_line[8] and branch_addresses_branch_name == address_id_and_line[6]:
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
            bulk_insert_users(users_data_and_customer_group, 0,
                              TENANT_ID, insert=True)

        if len(phone_number_and_customer_group) != 0:
            bulk_insert_phones(phone_number_and_customer_group, insert=True)

        if len(customer_group_addresses) != 0:
            customer_group_addresses = bulk_insert_customer_group_addresses(
                customer_group_addresses, bulk_insert_id, insert=True, select=True)
            if which_user == ORGAZANAIZATION:
                customer_group_using_primary_address = []
                for i in customer_group_addresses:
                    i = list(i)
                    i.append(TENANT_ID)
                    customer_group_using_primary_address.append(i)
                bulk_update_customer_group_using_primary_address(
                    customer_group_using_primary_address, insert=True)

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
            for coustomer_id, values in same_organization_diffrent_user.items():
                retrive_user = bulk_insert_users(
                    [], coustomer_id, TENANT_ID, select=True)
                for value in values:
                    email = False
                    if value[3] != None:
                        if value[3] in retrive_user["email_List"]:
                            email = True
                    if not email:
                        if value not in retrive_user["return_all_list"]:
                            value.append(TENANT_ID)
                            value.append(role_id)
                            value.append(created_by)
                            value.append(status)
                            value.append(hash_password)
                            value.append(datetime.datetime.now())
                            users_data_and_customer_group.append(tuple(value))
    return users_data_and_customer_group


def send_email(count, file_url, logo_url, target_email, filename=None,massege_type=""):
    with app.app_context():
        try:
            msg = Message('Feildy Message', sender=str(os.getenv('MAIL_SENDER')),
                          recipients=[target_email])
            with app.open_resource(file_url) as csv_file:
                msg.attach(filename=filename,
                           content_type="text/csv", data=csv_file.read())
            msg.html = email_template(count=count, logo_url=logo_url,massege_type=massege_type)
            mail.send(msg)
            return 'Email sent!'
        except Exception as e:
            return f" email : {str(e)}"


def send_error_thread(message, traceback, logo_url):
    def send_error_email(message, traceback, logo_url):
        with app.app_context():
            try:
                msg = Message('Feildy Error Message', sender=str(os.getenv('MAIL_SENDER')),
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


def api_call_for_cashe(TENANT_ID,customer_group_addresess_list):
    try:
        def call_for_cashe(TENANT_ID,customer_group_addresess_list):
            # params = {"type": TENANT_ID}
            # requests.get(
            #     'https://devgateway.getfieldy.com/z1/job/cache/flush', params=params)
            # requests.get(
            #     'https://devgateway.getfieldy.com/z1/accounting/cache/flush', params=params)
            # id_customer_group = []
            # for id in customer_group_addresess_list["retrive_customer_group"]:
            #     id_customer_group.append(id[0])
            # params = {"tenant_id":TENANT_ID,"id_customer_group":id_customer_group}
            # requests.post(
            #     'https://devgateway.getfieldy.com/z1/api/bulkupload/cache_update',params=params)
            return "Api Call Successfully"
        send_mail = threading.Thread(
            target=call_for_cashe, args=(TENANT_ID,customer_group_addresess_list))
        send_mail.start()
        return "Api Called"
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


if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0')
