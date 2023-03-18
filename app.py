from utils.json_condition import *
from utils.utils import *
from utils.query import *

import io
from flask import *
import csv
import datetime
import threading
from api.xlsx_to_csv import xlsx_convert_csv
from api.bulk_insert import bulk_insert
from flask_mail import Mail, Message
from template.email import email_template

app = Flask(__name__)

app.secret_key = SECRET_KEY
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USERNAME'] = 'muhammed@zaigoinfotech.com'
app.config['MAIL_PASSWORD'] = 'mlgbykepnxyraolt'
mail = Mail(app)

app.register_blueprint(xlsx_convert_csv, url_prefix='/api')
app.register_blueprint(bulk_insert, url_prefix='/api')


def send_email(count, file_url, logo_url, filename=None):
    with app.app_context():
        try:
            msg = Message('Feildy Message', sender='muhammed@zaigoinfotech.com',
                          recipients=['mrahil7510@gmail.com'])
            with app.open_resource(file_url) as csv_file:
                msg.attach(filename=filename,
                           content_type="text/csv", data=csv_file.read())
            msg.html = email_template(count=count)
            mail.send(msg)
            return 'Email sent!'
        except Exception as e:
            return str(e)


@app.route("/api/bulk_import", methods=['POST'])
def bulk_import_api():
    if request.method == 'POST':

        if 'file' not in request.files:
            return make_response(jsonify({'message': 'No file uploaded'}), 400)

        file = request.files['file']
        TENANT_ID = request.form.get('tanant_id', None)
        json_format = request.form.get('json_format', None)

        if TENANT_ID == None or json_format == None:
            return make_response(jsonify({'message': 'tanant_id or json_format is required'}), 400)

        import_sheet = file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(import_sheet))
        json_format = json.loads(json_format)
        which_user = which_user_types_imported(json_format)
        json_format = currect_json_map(json_format)
        json_format = add_validation_fields(json_format)
        json_count = len(json_format.keys())
        field_names = reader.fieldnames
        splite_field_name_with_json_count = field_names[0:json_count]

        contact_customer_list = []
        organization_customer_list = []
        contact_list = []
        organization_list = []
        invalid_data = []
        skip_data = []

        for line_index, line in enumerate(reader, 1):
            field_type = divide_to_field_type_with_json_format(
                line_index, line, splite_field_name_with_json_count, json_format, which_user)
            if len(field_type["contact"]) != 0:
                contact_customer_list.append(field_type["contact"])
            if len(field_type["organization"]) != 0:
                organization_customer_list.append(field_type["organization"])
            if len(field_type["contact_list"]) != 0:
                contact_list.append(field_type["contact_list"])
            if len(field_type["organization_list"]) != 0:
                organization_list.append(field_type["organization_list"])
            if len(field_type["invalid_data"]) != 0:
                invalid_data.append(field_type["invalid_data"])
            if len(field_type["skip_data"]) != 0:
                skip_data.append(field_type["skip_data"])

        send_mail_skip_data_and_invalid_data_convert_to_csv(
            splite_field_name_with_json_count, skip_data, invalid_data)

        tables_name = get_table_names_in_json_condition(json_format)
        contact_customer_list = table_name_use_suparat_all_data(
            contact_customer_list, tables_name)
        organization_customer_list = table_name_use_suparat_all_data(
            organization_customer_list, tables_name)

        bulk_insert_id = get_bulk_insert_id(select=True, insert=True)

        bulk_insert_using_bulk_type(
            TENANT_ID, bulk_insert_id, which_user,
            contact_customer_list, organization_customer_list,
            contact_list, organization_list)

        data = {
            'message': 'File imported successfully',
        }
        response = make_response(jsonify(data), 200)
        response.headers["Content-Type"] = "application/json"
        return response


# -------------------------------- step 1 --------------------------------

def divide_to_field_type_with_json_format(line_index, line, field_names, json_format, which_user):
    contact_customer_inner_list = []
    organization_customer_inner_list = []
    invalid_data = []
    contact_list = []
    organization_list = []
    all_contact = []
    all_organization = []
    skip_data = {}

    json_format_keys = json_format.keys()
    for column_index, key in enumerate(json_format_keys):
        user_type = json_format[key]['entity']

        field_name = field_names[column_index]
        value = line[field_name]
        if value == ".":
            value = ""

        if user_type == "contact":
            field_format_return_dict = finding_which_data(line_index,
                                                          user_type, json_format[key]['parent'], json_format[key]['table_slug'], json_format[key]['validation'], json_format[key]['field_type'], value, column_index)
            if field_format_return_dict["valid"]:
                contact_customer_inner_list.append((field_format_return_dict))
            else:
                invalid_data.append((field_format_return_dict))
                field_format_return_dict_copy = field_format_return_dict.copy()
                field_format_return_dict_copy["value"] = ""
                contact_customer_inner_list.append(
                    (field_format_return_dict_copy))

        if user_type == "organization":
            field_format_return_dict = finding_which_data(line_index,
                                                          user_type, json_format[key]['parent'], json_format[key]['table_slug'], json_format[key]['validation'], json_format[key]['field_type'], value, column_index)
            if field_format_return_dict["valid"]:
                organization_customer_inner_list.append(
                    (field_format_return_dict))
            else:
                invalid_data.append((field_format_return_dict))
                field_format_return_dict_copy = field_format_return_dict.copy()
                field_format_return_dict_copy["value"] = ""
                organization_customer_inner_list.append(
                    (field_format_return_dict_copy))

    add_new_field = add_new_field_based_on_user_type(
        line_index, contact_customer_inner_list, organization_customer_inner_list, which_user)
    contact_customer_inner_list = add_new_field["contact_customer_inner_list"]
    organization_customer_inner_list = add_new_field["organization_customer_inner_list"]

    if which_user in [CONTACT,CONTACT_AND_ORGAZANAIZATION]:
               
        contact_required_name = False
        contact_required_line_1 = False
        for contact in contact_customer_inner_list:
            if contact["column_name"] == "first_name":
                contact_required_name = len(contact["value"]) != 0
            if contact["column_name"] == "line_1":
                contact_required_line_1 = len(contact["value"]) != 0

        if contact_required_name and contact_required_line_1:
            contact_list = contact_customer_inner_list
            all_contact = organizing_with_table_name(line_index,
                                                    contact_customer_inner_list)
        else:
            skip_data.update({"contact": contact_customer_inner_list})

    if which_user in [ORGAZANAIZATION,CONTACT_AND_ORGAZANAIZATION]:

        organization_required_name = False
        organization_required_line_1 = False
        for organization in organization_customer_inner_list:
            if organization["column_name"] == "name":
                organization_required_name = len(organization["value"]) != 0
            if organization["column_name"] == "line_1":
                organization_required_line_1 = len(organization["value"]) != 0

        if organization_required_name and organization_required_line_1:
            organization_list = organization_customer_inner_list
            all_organization = organizing_with_table_name(line_index,
                                                        organization_customer_inner_list)
        else:
            skip_data.update({"organization": organization_customer_inner_list})

    contaxt = {"contact": all_contact,
               "organization": all_organization,
               "contact_list": contact_list,
               "organization_list": organization_list,
               "invalid_data": invalid_data,
               "skip_data": skip_data
               }
    return contaxt


def add_new_field_based_on_user_type(line_index, contact_customer_inner_list, organization_customer_inner_list, which_user):
    if which_user == CONTACT:
        contact_customer_inner_list.append(add_new_field(
            "contact", "customer_group", "customer_type", "contact_customer", line_index))
    if which_user == ORGAZANAIZATION:
        organization_customer_inner_list.append(add_new_field(
            "organization", "customer_group", "customer_type", "company_customer", line_index))
    if which_user == CONTACT_AND_ORGAZANAIZATION:
        contact_customer_inner_list.append(add_new_field(
            "contact", "customer_group", "customer_type", "contact_customer", line_index))
        organization_customer_inner_list.append(add_new_field(
            "organization", "customer_group", "customer_type", "company_customer", line_index))
    return {
        "contact_customer_inner_list": contact_customer_inner_list,
        "organization_customer_inner_list": organization_customer_inner_list
    }


def add_new_field(user_type, table_name, column_name, value, line_index):
    field_format_dict = {}
    field_format_dict.update(
        {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value, "valid": True, "line_number": line_index})
    return field_format_dict


def finding_which_data(line_index, user_type, table_name, column_name, validation, field_type, value, column_index):
    field_format_dict = {}
    valid = check_validation(validation, field_type, value)
    field_format_dict.update(
        {"user_type": user_type, "table_name": table_name, "column_name": column_name, "value": value, "valid": valid, "line_number": line_index, "column_number": column_index})
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
                    user_table_name_dict["user_type"] = item['user_type']

            # remove first_name and last_name field and add name field
            if user_table_name_dict["user_type"] == "contact":
                responce_dict["customer_group"].append(user_table_name_dict)
            responce_dict["customer_group"] = [d for d in responce_dict["customer_group"]
                                               if d['column_name'] not in ('first_name', 'last_name', 'job_title')]

            # add name field in users dictionary
            responce_dict["users"].append(user_table_name_dict)
        responce_list.append(responce_dict)
    return responce_list


# --------------------------------step 2 --------------------------------


def send_mail_skip_data_and_invalid_data_convert_to_csv(field_names, skip_data, invalid_data):
    field_names.insert(0, "line Number")
    if len(skip_data) != 0:
        send_mail_skip_data = threading.Thread(
            target=send_skipped_data, args=(field_names, skip_data))
        send_mail_skip_data.start()
    if len(invalid_data) != 0:
        send_mail_invalid_data = threading.Thread(
            target=send_invalid_data, args=(field_names, invalid_data))
        send_mail_invalid_data.start()
    return


def send_skipped_data(field_names, skip_data):
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
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="", filename=f"{csv_name}.csv")


def send_invalid_data(field_names, invalid_data):
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
        __file__)), 'invalid_data_sheets', f'{csv_name}.csv'), logo_url="", filename=f"{csv_name}.csv")

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


def bulk_insert_using_bulk_type(TENANT_ID, bulk_insert_id, which_user, contact_customer_list=[], organization_customer_list=[], contact_list=[], organization_list=[]):
    if which_user == CONTACT:
        contact_customer_group_addresess_list = bulk_insert_function(
            contact_customer_list, TENANT_ID, bulk_insert_id, custemer_type="contact_customer")

        if contact_customer_group_addresess_list["phones_and_users_data_import_true_or_false"]:
            contact = threading.Thread(target=contact_thread, args=(
                contact_list, contact_customer_group_addresess_list, TENANT_ID))
            contact.start()

    if which_user == ORGAZANAIZATION:
        organization_customer_group_addresess_list = bulk_insert_function(
            organization_customer_list, TENANT_ID, bulk_insert_id, custemer_type="company_customer")

        if organization_customer_group_addresess_list["phones_and_users_data_import_true_or_false"]:
            organization = threading.Thread(target=organization_thread, args=(
                organization_list, organization_customer_group_addresess_list, TENANT_ID))
            organization.start()

    if which_user == CONTACT_AND_ORGAZANAIZATION:
        contact_customer_group_addresess_list = bulk_insert_function(
            contact_customer_list, TENANT_ID, bulk_insert_id, custemer_type="contact_customer")
        organization_customer_group_addresess_list = bulk_insert_function(
            organization_customer_list, TENANT_ID, bulk_insert_id, custemer_type="company_customer")

        if contact_customer_group_addresess_list["phones_and_users_data_import_true_or_false"]:
            contact = threading.Thread(target=contact_thread, args=(
                contact_list, contact_customer_group_addresess_list, TENANT_ID))
            contact.start()

        if organization_customer_group_addresess_list["phones_and_users_data_import_true_or_false"]:
            organization = threading.Thread(target=organization_thread, args=(
                organization_list, organization_customer_group_addresess_list, TENANT_ID))
            organization.start()
    return


def bulk_insert_function(bulk_insert_list, TENANT_ID, bulk_insert_id, custemer_type=None):
    retrive_customer_group_data_use_bulk_insert_id = []
    retrive_addresses_data_use_bulk_insert_id = []
    phones_and_users_data_import_true_or_false = False

    for table_name, all_values in bulk_insert_list.items():
        if table_name == "customer_group" or table_name == "addresses":
            get_single_value_for_table_names = all_values[0] if len(
                all_values) != 0 else []
            column_names = get_column_names(
                table_name, get_single_value_for_table_names)
            values = get_values(table_name, TENANT_ID,
                                all_values, bulk_insert_id)
            if len(values) != 0:
                if table_name == "customer_group":
                    bulk_insert_dynamic(
                        table_name, column_names, values, insert=True)
                    retrive_customer_group_data_use_bulk_insert_id = retrive_customer_group_and_addresses_data_use_bulk_insert_id(
                        table_name, bulk_insert_id, custemer_type, select_customer_group=True)
                    create_avatar_then_dumb_files_db_and_map_customer_group = threading.Thread(
                        target=create_avatar_then_dumb_files_db_and_map_customer_group_thread, args=(retrive_customer_group_data_use_bulk_insert_id, bulk_insert_id,))
                    create_avatar_then_dumb_files_db_and_map_customer_group.start()
                if table_name == "addresses":
                    bulk_insert_dynamic(
                        table_name, column_names, values, insert=True)
                    retrive_addresses_data_use_bulk_insert_id = retrive_customer_group_and_addresses_data_use_bulk_insert_id(
                        table_name, bulk_insert_id, select_address=True)
                phones_and_users_data_import_true_or_false = True
    responce = {
        "retrive_customer_group": retrive_customer_group_data_use_bulk_insert_id,
        "retrive_addresses": retrive_addresses_data_use_bulk_insert_id,
        "phones_and_users_data_import_true_or_false": phones_and_users_data_import_true_or_false
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


def contact_thread(contact_list, contact_customer_group_addresess_list, TENANT_ID):
    users_and_phones_map_list(
        contact_list, contact_customer_group_addresess_list, TENANT_ID)


def organization_thread(organization_list, organization_customer_group_addresess_list, TENANT_ID):
    users_and_phones_map_list(
        organization_list, organization_customer_group_addresess_list, TENANT_ID)


def users_and_phones_map_list(sheet_row_ways_contact_or_organization_list, retrive_db_customer_group_or_address_list, TENANT_ID):
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
            if value["column_name"] == "name":
                name = value["value"]
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
        customer_group_addresses.append(
            tuple(customer_group_pk_and_address_pk))

    # error_data_for_customer_group_addresses = []
    # max_length = max(map(len, customer_group_addresses))
    # for index, item in enumerate(customer_group_addresses):
    #     if len(item) != max_length:
    #         error_data_for_customer_group_addresses.append(item)
    #         customer_group_addresses.pop(index)

    bulk_insert_customer_group_addresses(
        customer_group_addresses, id_address, insert=True)
    bulk_insert_users(users_data_and_customer_group, insert=True)
    bulk_insert_phones(phone_number_and_customer_group, insert=True)
    return


if __name__ == "__main__":
    app.run(debug=True)
