
from typing import Dict
import pandas as pd
import re



def add_validations_in_product_json_format(json_format : Dict[str,Dict[str,str]]):
	for column_name, other_informations in json_format.items(): 
		table_column_slug = other_informations['table_column_slug']
		validation = fields_validation(table_column_slug)
		other_informations['validation'] = validation
	return json_format

def fields_validation(table_column_slug):
    product_validation = {}
    product_validation['name'] = {"validation": length_validation(
            max=256), "field_type": "all_characters"}
    product_validation['sku'] = {"validation": length_validation(
            max=64), "field_type": "special_characters"}
    product_validation['hsn'] = {"validation": length_validation(
            max=44), "field_type": "special_characters"}
    product_validation['description'] = {"validation": length_validation(
            max=20000), "field_type": "all_characters"}
    product_validation['price'] = {"validation": length_validation(
            max=6), "field_type": "price_and_stock_characters"}
    product_validation['current_stock'] = {"validation": length_validation(
            max=12), "field_type": "price_and_stock_characters"}
    product_validation['low_stock'] = {"validation": length_validation(
            max=12), "field_type": "price_and_stock_characters"}
    return product_validation[table_column_slug]


def length_validation(min=0, max=256):
    validation = {"min": min, "max": max}
    return validation


def valid_value(value):
    value = "" if value == "." else value
    value = "" if pd.isna(value) else value
    value = str(value) 
    value = value if len(value) != 0 else ""
    return value


def validate_field_type_value(value, validation):
    valid, msg = False, "No Value"
    if len(value) != 0:
        field_type = validation['field_type']
        if field_type == "all_characters" :
            valid, msg = all_characters_validation(validation, value)
        if field_type == "special_characters" :
            valid, msg = special_characters_validation(validation, value)
        if field_type == "price_and_stock_characters" :
            valid, msg = price_and_stock_characters_validation(validation, value)
    return valid, msg


def all_characters_validation(validation, value):
    max = validation['validation']['max']
    if len(str(value)) > max:
        return 	True, f"Not Allowed More than {max} characters"
    else:
        return	False, ""


def special_characters_validation(validation, value):
    max = validation['validation']['max']
    pattern = r'^[a-zA-Z0-9_:\s&-]{1,%d}$' % max
    response = {}
    if re.match(pattern, value) is None:
        return 	True, f"Invalid format (allowed only '- _ : & ' special characters) or Not Allowed More than {max} characters"
    else:
        return	False , ""


def price_and_stock_characters_validation(validation, value):
    max = validation['validation']['max']
    pattern = r'^\d{1,%d}(?:\.\d{1,2})?$' % max
    response = {}
    if re.match(pattern, value) is None:
        return 	True, f"1- Not Allowed More than {max} characters, 2- Not Allowed More than {max} characters before decimal point, 3 - Not Allowed More than 2 characters after decimal point"
    else:
        return	False , ""

