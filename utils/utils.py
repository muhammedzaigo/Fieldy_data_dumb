import random
import string
import datetime
import avinit
import bcrypt
import os
import re
from dotenv import load_dotenv
import pandas as pd
import io
import chardet

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
            try:
                avinit.get_png_avatar(
                    name[0], output_file=f'{UPLOAD_FOLDER}/{name[1]}')
            except:
                continue
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


def currect_json_map_and_which_user_type(json_format):
    organization = False
    which_types_imported = CONTACT
    json_format = currect_address_map(json_format)

    for key, value in json_format.items():
        if value["table_slug"] == "zipcode":
            value["table_slug"] = "zip_code"

        # contact and organization address change to branch_address
        if value["parent"] == "addresses":
            if value["table_slug"] in ["line_1", "line_2", "city", "state", "zip_code", "branch_name", "first_name", "last_name",]:
                value["parent"] = "branch_addresses"

        if value["parent"] == "":
            if value["table_slug"] in ["name", "website", "email", "first_name", "last_name", "lead_source"]:
                value["parent"] = "customer_group"

            if value["table_slug"] == "job_title":
                value["parent"] = "users"

            if value["table_slug"] in ["line_1", "line_2", "city", "state", "zip_code", "branch_name"]:
                value["parent"] = "addresses"

        if value["parent"] == "contacts":
            value["parent"] = "users"

        if value["entity"] == "organization":
            organization = True
        json_format = add_validation_fields(value, key, json_format)
    if organization:
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

    if branch_address and address:
        return json_format

    for key, value in json_format.items():
        if address and not branch_address:
            if value["parent"] == "addresses":
                if value["table_slug"] in ["line_2", "city", "state", "zip_code", "branch_name", "first_name", "last_name",]:
                    value["parent"] = ""

        if not address and branch_address:
            if value["parent"] == "":
                if value["table_slug"] in ["line_2", "city", "state", "zip_code", "branch_name"]:
                    value["parent"] = "addresses"
    return json_format


def add_validation_fields(values, key, json_format):
    if values["table_slug"] == "line_1":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "all_characters"})

    if values["table_slug"] == "line_2":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "all_characters"})

    if values["table_slug"] == "state":
        json_format[key].update({"validation": validation(
            max=45), "field_type": "all_characters"})

    if values["table_slug"] == "city":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "all_characters"})

    if values["table_slug"] == "zip_code":
        json_format[key].update({"validation": validation(
            max=18), "field_type": "all_characters"})

    if values["table_slug"] == "email":
        json_format[key].update({"validation": validation(
            min=64, max=256), "field_type": "email"})

    if values["table_slug"] == "number":
        json_format[key].update({"validation": validation(
            min=6, max=20), "field_type": "number"})

    if values["table_slug"] == "phone":
        json_format[key].update({"validation": validation(
            min=6, max=20), "field_type": "number"})

    if values["table_slug"] == "lead_source":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "alpha_numeric"})

    if values["table_slug"] == "branch_name":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "all_characters"})

    if values["table_slug"] == "first_name":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "alpha_numeric"})

    if values["table_slug"] == "last_name":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "alpha_numeric"})

    if values["table_slug"] == "job_title":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "alpha_numeric"})

    if values["table_slug"] == "name":
        json_format[key].update({"validation": validation(
            max=256), "field_type": "alpha_numeric"})

    if values["table_slug"] == "website":
        json_format[key].update({"validation": validation(
            max=512), "field_type": "website"})
    return json_format


def validation(min=0, max=256):
    validation = {"min": min, "max": max}
    return validation


def is_valid_email(email, min, max):
    response = {}
    pattern = r"^[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,256}\.[a-zA-Z]{2,}$"
    if re.match(pattern, email):
        response.update({"valid": True})
    else:
        response.update({"valid": False, "message": "Invalid email address"})
    return response


def is_valid_url(url, min=1, max=256):
    response = {}
    pattern = r"^(?:(?:https?|ftp):\/\/)?(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\/.*)?$"
    if re.match(pattern, url) and len(url) <= max:
        response.update({"valid": True})
    else:
        response.update(
            {"valid": False, "message": f"Invalid url or your provide greater than {max} characters"})
    return response


def is_valid_phone_number(number, min=6, max=15):
    converted_number = re.sub(r'[^0-9]', '', str(number))
    pattern = r"^\+?[1-9]\d{%d,%d}$" % (min-1, max-1)
    response = {}
    serch_pattern = r"[\.e]" 
    if re.search(serch_pattern, str(number)):
        response.update(
            {"valid": False, "message": f"Invalid phone number"})
    else:
        if re.match(pattern, converted_number):
            if len(converted_number) >= min and len(converted_number) <= max:
                response.update({"valid": True})
        else:
            response.update(
                {"valid": False, "message": f"Invalid phone number or phone number must be minimum {min} and maximam {max} characters"})
    return response


def is_valid_alphanumeric(text, min=1, max=256):
    response = {}
    pattern = r"[a-zA-Z0-9]+"
    if re.match(pattern, text) and len(text) <= max:
        response.update({"valid": True})
    else:
        response.update(
            {"valid": False, "message": f"Invalid format or your provide greater than {max} characters"})
    return response


def is_all_characters(text, min=1, max=256):
    response = {}
    if len(str(text)) > max:
        response.update(
            {"valid": False, "message": f"Invalid format or your provide greater than {max} characters"})
    else:
        response.update({"valid": True})
    return response


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


def remove_duplicates_in_sheet(sheet, which_user, json_format):
    if which_user == ORGAZANAIZATION:
        context = organization_remove_duplicates_in_sheet(sheet, json_format)
    else:
        context = contact_remove_duplicates_in_sheet(sheet)
    return context


def contact_remove_duplicates_in_sheet(read_sheet):
    df = pd.read_csv(io.StringIO(read_sheet))
    df.columns = map(str.lower, df.columns)
    df = df.fillna('')
    if 'email' in df.columns and df['email'].notnull().any():
        # filter out rows with empty email fields
        df_with_emails = df[df['email'].notnull()]
        cleaned_data_with_emails = df_with_emails.drop_duplicates(
            subset=df.columns.difference(['email']), keep='first')
        cleaned_data = pd.concat([cleaned_data_with_emails,
                                  df[df['email'].isnull()]]).drop_duplicates(keep='first')
    else:
        cleaned_data = df.drop_duplicates(keep='first')

    removed_data = df[~df.isin(cleaned_data)].dropna(how='all')
    removed_data = removed_data.dropna(how='all')
    cleaned_data_dict = cleaned_data.to_dict(orient='records')
    fieldnames = list(cleaned_data_dict[0].keys())
    removed_data_dict = removed_data.to_dict(orient='index')
    context = {"cleaned_data": cleaned_data_dict,
               "fieldnames": fieldnames, "removed_rows": removed_data_dict}
    return context


def organization_remove_duplicates_in_sheet(read_sheet, json_format):
    uniqe_fieldname = "organization_name"
    for key, value in json_format.items():
        if value["table_slug"] == "name":
            uniqe_fieldname = value["sheet_header_name"]
            uniqe_fieldname = uniqe_fieldname.lower()
            break

    df = pd.read_csv(io.StringIO(read_sheet))

    df.columns = map(str.lower, df.columns)
    df = df.fillna('')
    if 'email' in df.columns and not df['email'].isnull().all():

        # cleaned_data = df.drop_duplicates(
        #     subset=df.columns.difference(['email']), keep='first')
        # cleaned_data.drop_duplicates(
        #     subset=['email'], keep='first', inplace=True)
        cleaned_data = df.drop_duplicates(keep='first')

        removed_data = df[~df.isin(cleaned_data)].dropna(how='all')
        removed_data = removed_data.dropna(how='all')

        dupicate_name = organization_dupicate_name(
            df, cleaned_data, uniqe_fieldname)
        cleaned_data = dupicate_name["cleaned_data"]
        remove_dupicate_name = dupicate_name["name_removed_data"]
    else:
        cleaned_data = df.drop_duplicates(keep='first')
        removed_data = df[~df.isin(cleaned_data)].dropna(how='all')
        removed_data = removed_data.dropna(how='all')

        dupicate_name = organization_dupicate_name(
            df, cleaned_data, uniqe_fieldname)
        cleaned_data = dupicate_name["cleaned_data"]
        remove_dupicate_name = dupicate_name["name_removed_data"]

    cleaned_data_dict = cleaned_data.to_dict(orient='records')
    removed_data_dict = removed_data.to_dict(orient='index')
    remove_dupicate_name_dict = remove_dupicate_name.to_dict(orient='records')
    fieldnames = list(cleaned_data_dict[0].keys())
    context = {"cleaned_data": cleaned_data_dict,
               "fieldnames": fieldnames, "removed_rows": removed_data_dict, "remove_dupicate_name_dict": remove_dupicate_name_dict}
    return context


def organization_dupicate_name(df, cleaned_data, uniqe_fieldname):
    cleaned_data = df.drop_duplicates(
        subset=[uniqe_fieldname], keep='first')
    name_removed_data = df[~df.isin(cleaned_data)].dropna(how='all')
    return {"cleaned_data": cleaned_data, "name_removed_data": name_removed_data}


def import_sheets(file):
    try:
        import_sheet = file.read()
        file_encoding = chardet.detect(import_sheet)['encoding']
        import_sheet = import_sheet.decode(file_encoding)
        csv_filename = ""
        XLSX = False
    except:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        workbook = pd.ExcelFile(file)
        sheet_name = workbook.sheet_names[0]
        sheet_data = workbook.parse(sheet_name)
        if not os.path.exists("sheets"):
            os.mkdir("sheets")
        csv_filename = f"sheets/{sheet_name}_{timestamp}.csv"
        sheet_data.to_csv(csv_filename, index=False)
        with open(csv_filename, 'rb') as csv_file:
            encoding_type = chardet.detect(csv_file.read())
        with open(csv_filename, 'r', encoding=encoding_type['encoding']) as csv_file:
            import_sheet = csv_file.read()
        XLSX = True

    return {
        "csv_filename": csv_filename,
        "XLSX": XLSX,
        "import_sheet": import_sheet
    }


def delete_csv_file(import_sheet):
    if import_sheet["XLSX"]:
        csv_filename = import_sheet["csv_filename"]
        if os.path.exists(csv_filename):
            os.remove(csv_filename)
        return "Successfully deleted"



def import_sheet_convert_to_csv(sheet, orient="records", unique_values=[]):
    try:
        import_sheet = sheet.read()
        file_encoding = chardet.detect(import_sheet)['encoding']
        import_sheet = import_sheet.decode(file_encoding)
        df = pd.read_csv(io.StringIO(import_sheet))
        df.columns = df.columns.str.replace('\n', '')
        df.columns = map(str.lower, df.columns)
        df.columns = map(str.strip, df.columns)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.fillna('')
        cleaned_data = df.drop_duplicates(keep='first')
        if len(unique_values) > 0:
            for unique in unique_values:
                if unique in df.columns:
                    cleaned_data = cleaned_data.drop_duplicates(subset=[unique], keep="first")
        cleaned_data_dict = cleaned_data.to_dict(orient=orient)
    except:
        df = pd.read_excel(sheet)
        df.columns = df.columns.str.replace('\n', '')
        df.columns = map(str.lower, df.columns)
        df.columns = map(str.strip, df.columns)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.fillna('')
        cleaned_data = df.drop_duplicates(keep='first')
        if len(unique_values) > 0:
            for unique in unique_values:
                if unique in df.columns:
                    cleaned_data = cleaned_data.duplicated(subset=[unique], keep="first")
        cleaned_data_dict = cleaned_data.to_dict(orient=orient)
    return cleaned_data_dict, df
