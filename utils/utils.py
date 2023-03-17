import random
import string
import datetime
import avinit
import bcrypt
import threading
import os
import re
import pandas as pd
from utils.query import *

UPLOAD_FOLDER = "media"
SALT = b'$2y$10$/XihfLhBx5RphDLAxfldkOdyEy6seEfWuA1oGGkfNYslabtmYndT'
DEFAULT_PASSWORD = b'Fieldy@123'
FIELDY_AT_123 = "$2y$10$/XihfLhBx5RphDLAxfldkOyPDO4YAv9YaGPtzmN/LvUpUTCkdlA82"
TENANT_ID = 15
MIME = "image/png"
SECRET_KEY = "f#6=zf!2=n@ed-=6g17&k4e4fl#d4v&l*v6q5_6=8jz1f98v#"

CONTACT = 1
ORGAZANAIZATION = 2
CONTACT_AND_ORGAZANAIZATION = 3


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
        hashed_password = bcrypt.hashpw(password, SALT)
        hashed_password = hashed_password.decode()
    except Exception as e:
        hashed_password = FIELDY_AT_123
        print(str(e))
    return hashed_password


# (`id_customer_group`,`email`,`name`)
def create_avatar_then_dumb_files_db_and_map_customer_group_thread(customer_group_id_and_emails: tuple = (), bulk_insert_id: int = 1):
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
            if file_items[0] == file_identifier:  # file_identifier == file_identifier
                update_file_id_custemer_group.append(
                    (file_items[1], file_id, TENANT_ID, datetime.datetime.now()))  # `id_customer_group_id`,`id_file`
                continue
    bulk_update_customer_with_file = bulk_update_customer_group(
        update_file_id_custemer_group, insert=True)
    return


def currect_json_map(json_format):
    for key, value in json_format.items():
        if value["table_slug"] in ["name", "website", "email", "first_name", "last_name", "job_title"]:
            value["parent"] = "customer_group"
        if value["table_slug"] == "zipcode":
            value["table_slug"] = "zip_code"
        if value["table_slug"] in ["line_1", "line_2", "city", "state", "zip_code", "branch_name"]:
            value["parent"] = "addresses"
        if value["table_slug"] in ["number"]:
            value["parent"] = "phones"
    return json_format


def which_user_types_imported(json_format):
    which_types_imported = CONTACT
    which_types = []
    for key, values in json_format.items():
        which_types.append(values['entity'])
    which_types = list(set(which_types))
    if len(which_types) != 0 and "contact" not in which_types:
        which_types_imported = ORGAZANAIZATION
    if "contact" in which_types and "organization" in which_types:
        which_types_imported = CONTACT_AND_ORGAZANAIZATION
    return which_types_imported


def add_validation_fields(json_format):
    for key, values in json_format.items():
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
        if values["table_slug"] == "lead_source":
            json_format[key].update({"validation": validation(
                max="256"), "field_type": "alpha_numeric"})
        if values["entity"] == "contact":
            if values["table_slug"] == "first_name":
                json_format[key].update({"validation": validation(
                    max="256"), "field_type": "alpha_numeric"})
            if values["table_slug"] == "last_name":
                json_format[key].update({"validation": validation(
                    max="256",), "field_type": "alpha_numeric"})
            if values["table_slug"] == "job_title":
                json_format[key].update(
                    {"validation": validation(max="256"), "field_type": "number"})
        if values["entity"] == "organization":
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
    return  True if len(number) >= min and len(number) <= max else False


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
    if "customer_group" in table_names:
        table_names.append('users')
    return table_names