import random
import string
import datetime
import avinit
import bcrypt
import threading
import os
import re
from dotenv import load_dotenv
from utils.query import bulk_insert_files, bulk_update_customer_group

load_dotenv()

UPLOAD_FOLDER = "media"
# SALT = bytes(str(os.getenv('SALT')),'utf-8')
SALT = b"$2y$10$/XihfLhBx5RphDLAxfldkOdyEy6seEfWuA1oGGkfNYslabtmYndT"
DEFAULT_PASSWORD = 'Fieldy@123'
FIELDY_AT_123 = str(os.getenv('FIELDY_AT_123'))
MIME = "image/png"

CONTACT = 1
ORGAZANAIZATION = 2


def create_avatar(names, create=False):
    if create:
        if not os.path.exists(UPLOAD_FOLDER):
            os.mkdir(UPLOAD_FOLDER)
        for name in names:
            avinit.get_png_avatar(
                name[0], output_file=f'{UPLOAD_FOLDER}/{name[1]}')
        return "avatars created"


def random_string(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def password_hash(password):
    try:
        hashed_password = bcrypt.hashpw(password.encode(), SALT)
        hashed_password = hashed_password.decode()
    except Exception as e:
        hashed_password = FIELDY_AT_123
        print(f"password - {str(e)}")
    return hashed_password


# (`id_customer_group`,`email`,`name`)
def create_avatar_then_dumb_files_db_and_map_customer_group_thread(customer_group_id_and_emails: tuple = (), bulk_insert_id: int = 1, TENANT_ID=0):
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
                    (file_items[1], file_id, TENANT_ID, datetime.datetime.now()))
                continue
    bulk_update_customer_group(update_file_id_custemer_group, insert=True)
    return "File Upload Successfully"


def currect_json_map_and_which_user_type(json_format):
    organization = False
    which_types_imported = CONTACT
    which_types = []
    json_format = currect_address_map(json_format)
    for key, value in json_format.items():
        if value["table_slug"] == "zipcode":
            value["table_slug"] = "zip_code"

        if value["parent"] == "addresses":
            if value["table_slug"] in ["line_1", "line_2", "city", "state", "zip_code", "branch_name", "first_name", "last_name",]:
                value["parent"] = "branch_addresses"

        if value["parent"] == "":
            if value["table_slug"] in ["name", "website", "email", "first_name", "last_name", "job_title", "lead_source"]:
                value["parent"] = "customer_group"

            if value["table_slug"] in ["line_1", "line_2", "city", "state", "zip_code", "branch_name"]:
                value["parent"] = "addresses"

        if value["parent"] == "contacts":
            value["parent"] = "users"

        if value["entity"] == "organization" and value["table_slug"] == "name":
            organization = True

        which_types.append(value['entity'])

        add_validation_fields(value, key, json_format)

    if organization:
        json_format = if_name_in_organization_all_entity_convert_organization(
            json_format)

    which_types = list(set(which_types))
    if "contact" not in which_types and len(which_types) != 0:
        which_types_imported = ORGAZANAIZATION
    if "contact" in which_types and "organization" in which_types and organization:
        which_types_imported = ORGAZANAIZATION

    return {"json_format": json_format, "user_type": which_types_imported}


def currect_address_map(json_format):
    address = False
    branch_address = False
    for key, value in json_format.items():
        if value["table_slug"] == "line_1" and value["parent"] == "":
            address = True
        if value["table_slug"] == "line_1" and value["parent"] == "addresses":
            branch_address = True

    for key, value in json_format.items():

        if address and branch_address == False:
            if value["parent"] == "addresses" and value["table_slug"] in ["line_2", "city", "state", "zip_code", "branch_name", "first_name", "last_name",]:
                value["parent"] = ""

        if address == False and branch_address:
            if value["parent"] == "" and value["table_slug"] in ["line_2", "city", "state", "zip_code", "branch_name"]:
                value["parent"] = "addresses"

    return json_format


def add_validation_fields(values, key, json_format):
    if values["table_slug"] == "line_1":
        json_format[key].update({"validation": validation(
            max="255"), "field_type": "all_characters"})

    if values["table_slug"] == "line_2":
        json_format[key].update({"validation": validation(
            max="150"), "field_type": "all_characters"})

    if values["table_slug"] == "state":
        json_format[key].update({"validation": validation(
            max="255"), "field_type": "all_characters"})

    if values["table_slug"] == "city":
        json_format[key].update({"validation": validation(
            max="255"), "field_type": "all_characters"})

    if values["table_slug"] == "zip_code":
        json_format[key].update({"validation": validation(
            max="18"), "field_type": "all_characters"})

    if values["table_slug"] == "email":
        json_format[key].update({"validation": validation(
            min="64", max="255"), "field_type": "email"})

    if values["table_slug"] == "number":
        json_format[key].update({"validation": validation(
            min="6", max="15"), "field_type": "number"})

    if values["table_slug"] == "phone":
        json_format[key].update({"validation": validation(
            min="6", max="15"), "field_type": "number"})

    if values["table_slug"] == "lead_source":
        json_format[key].update({"validation": validation(
            max="256"), "field_type": "alpha_numeric"})
    if values["table_slug"] == "branch_name":
        json_format[key].update({"validation": validation(
            max="256"), "field_type": "all_characters"})

    if values["table_slug"] == "first_name":
        json_format[key].update({"validation": validation(
            max="256"), "field_type": "alpha_numeric"})

    if values["table_slug"] == "last_name":
        json_format[key].update({"validation": validation(
            max="256",), "field_type": "alpha_numeric"})

    if values["table_slug"] == "job_title":
        json_format[key].update(
            {"validation": validation(max="256"), "field_type": "number"})

    if values["table_slug"] == "name":
        json_format[key].update({"validation": validation(
            max="256"), "field_type": "alpha_numeric"})

    if values["table_slug"] == "website":
        json_format[key].update(
            {"validation": validation(max="512"), "field_type": "website"})

    if values["table_slug"] == "id_country":
        json_format[key].update({"validation": validation(
            max="90"), "field_type": "all_characters"})

    return json_format


def if_name_in_organization_all_entity_convert_organization(json_format):
    for key, value in json_format.items():
        if value["entity"] != "organization":
            value["entity"] = "organization"
    return json_format


def validation(min: str = "", max: str = ""):
    validation = {"min": min, "max": max}
    return validation


def is_valid_email(email, min, max):
    pattern = r"^[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,255}\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def is_valid_url(url, min=1, max=256):
    pattern = r"^(?:(?:https?|ftp):\/\/)?(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\/.*)?$"
    return bool(re.match(pattern, url)) and len(url) <= max


def is_valid_phone_number(number, min=6, max=15):
    # pattern = r"^\+?[1-9]\d{%d,%d}$" % (min-1, max-1)
    # return bool(re.match(pattern, number))
    return True if len(number) >= min and len(number) <= max else False


def is_valid_alphanumeric(text, min=1, max=256):
    pattern = r"[a-zA-Z0-9]+"
    return bool(re.match(pattern, text)) and len(text) <= max


def is_all_characters(text, min=1, max=256):
    return len(text) <= max


def get_table_names_in_json_condition(json_format):
    table_names = []
    for key in json_format.keys():
        table_name = json_format[key]['parent']
        table_names.append(table_name)
    table_names = list(set(table_names))
    if "users" not in table_names:
        if "customer_group" in table_names:
            table_names.append('users')
    return table_names


def get_column_names(table_name, bulk_insert_values):
    column_names = []
    for column_name in bulk_insert_values:
        column_names.append(column_name['column_name'])
    column_names = add_column_name(table_name, column_names)
    return column_names


def add_column_name(table_name, column_names):
    if table_name in ["addresses", "branch_addresses"]:
        column_names.append("id_tenant")
    if table_name in ["phones", "customer_group"]:
        column_names.append("tenant_id")
    column_names.append("created_at")
    if table_name == "users":
        column_names.append("created_by")
    if table_name in ["addresses", "customer_group", "branch_addresses"]:
        column_names.append("bulk_insert_id")
    return column_names


def get_column_values(table_name, TENANT_ID, bulk_insert_values, bulk_insert_id, created_by):
    values = []
    for bulk_insert_value in bulk_insert_values:
        val = []
        for items in bulk_insert_value:
            val.append(items['value'])
        val = add_column_values(
            val, table_name, TENANT_ID, bulk_insert_id, created_by)
        values.append(tuple(val))
    return values


def add_column_values(val, table_name, TENANT_ID, bulk_insert_id, created_by):
    val.append(TENANT_ID)
    val.append(datetime.datetime.now())
    if table_name == "users":
        val.append(created_by)
    if table_name in ["addresses", "customer_group", "branch_addresses"]:
        val.append(bulk_insert_id)
    return val
