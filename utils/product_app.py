
import datetime
from decimal import Decimal
from utils.product_utils import *
from utils.query import bulk_insert_dynamic, retrive_products_use_bulk_insert_id, update_items_current_stock


def read_single_product(line_index, single_row, json_format, existing_products, context):

    skipped_data = []
    product_import_data = []
    price_import_data = []
    product_already_exists = False
    product_name_is_empty = False
    product_already_exists_dict = {}

    for sheet_column_name in json_format.keys():
        column_deatails = json_format[sheet_column_name]
        column_name = str(sheet_column_name).lower()
        value = single_row[column_name]
        table_column_slug = column_deatails['table_column_slug']
        column_index = column_deatails['column_index']
        validation = column_deatails['validation']
        value = valid_value(value)
        if table_column_slug  == 'name':
            if len(value) == 0:
                product_name_is_empty = True
                break
            if str(value).lower().strip() in existing_products:
                product_already_exists = True
        
        if not product_already_exists:
            skip, msg = validate_field_type_value(value, validation)
            if skip:
                skipped_data.append(
                    {
                        "column_name": sheet_column_name,
                        "value" : value,
                        "row_index": line_index,
                        "column_index": column_index,
                        "message" : msg
                    }
                )
                value = ""
            dumbed_data_dict = for_dumbed_data_fun(value, table_column_slug)
            if table_column_slug == "price":
                price_import_data.append(dumbed_data_dict)
            else:
                product_import_data.append(dumbed_data_dict)
        else:
            if table_column_slug  not in ["name","current_stock"]:
                continue
            if table_column_slug  == 'name':
                product_already_exists_dict["name"] = value
            if table_column_slug == 'current_stock':
                product_already_exists_dict["current_stock"] = value
    
    if not product_already_exists and not product_name_is_empty:
        
        add_new_column_and_values_in_product('items', 'id_tenant', context.get('TENANT_ID'), product_import_data)
        add_new_column_and_values_in_product('items', 'is_product', 1, product_import_data)
        add_new_column_and_values_in_product('items', 'bulk_insert_id', context.get('bulk_insert_id'), product_import_data)
        add_new_column_and_values_in_product('items', 'bulk_insert_row_number', line_index, product_import_data)
        
        add_new_column_and_values_in_product('item_prices', 'bulk_insert_row_number', line_index, price_import_data)
        add_new_column_and_values_in_product('item_prices', 'bulk_insert_id', context.get('bulk_insert_id'), price_import_data)
        add_new_column_and_values_in_product('item_prices', 'item_id', 0 , price_import_data)
    

    context_data = {
        "product_already_exists":product_already_exists, 
        "product_name_is_empty":product_name_is_empty,
        "skipped_data": skipped_data,
        "product_import_data":product_import_data,
        "price_import_data":price_import_data,
        "product_already_exists_dict" : product_already_exists_dict
    }
    return context_data


def add_new_column_and_values_in_product(table_name, column_name, value, append_list :list):
    return append_list.append(
        {
            "table_name": table_name,
            "column_name": column_name,
            "value" : value,
        }
    )
    

def for_dumbed_data_fun(value : str, table_column_slug : str):
    dumbed_data_dict = {}
    table_name = "items"
    if table_column_slug == "price":
        table_name = "item_prices"
        dumbed_data_dict = {
                "table_name": table_name,
                "column_name": table_column_slug,
                "value" : value,
        }
    else:
        dumbed_data_dict = {
            "table_name": table_name,
            "column_name": table_column_slug,
            "value" : value,
        }
    return dumbed_data_dict



def product_bulk_import_function(product_import_data_list, price_import_data_list,context,existing_products, update_products):
    if len(product_import_data_list) > 0:
        table_name ,column_names = get_product_column_names(product_import_data_list[0])
        column_values = get_product_column_values(product_import_data_list)
        bulk_insert_dynamic(table_name, column_names, column_values, insert=True)
        bulk_insert_id = context.get('bulk_insert_id')
        retrive_products = retrive_products_use_bulk_insert_id(bulk_insert_id)
        key_value_products_dict = retrive_products_convert_to_key_value_pair(retrive_products)
        if len(price_import_data_list) > 0:
            price_import_data_list = priceitems_add_product_id_by_using_row_number(price_import_data_list, key_value_products_dict)
            table_name ,column_names = get_product_column_names(price_import_data_list[0])
            column_values = get_product_column_values(price_import_data_list)
            bulk_insert_dynamic(table_name, column_names, column_values, insert=True)

    update_current_stock_on_items( existing_products,update_products)
    return "Successfully inserted"


def get_product_column_names(import_data_list):
    column_names = []
    table_name = None
    for column_name in import_data_list:
        if table_name is None:
            table_name = column_name['table_name']
        column_names.append(column_name['column_name'])
    return table_name, column_names


def get_product_column_values(import_data_list):
    outerlist = []
    for data_list in import_data_list:
        innerlist = []
        for items in data_list:
            innerlist.append(items['value'])
        outerlist.append(innerlist)
    return outerlist


def retrive_products_convert_to_key_value_pair(retrive_products):
    key_value_products_dict = {}
    for item in retrive_products:
        key_value_products_dict[item[1]] = item[0]
    return key_value_products_dict


def priceitems_add_product_id_by_using_row_number(price_import_data_list, key_value_products_dict):
    for prices in price_import_data_list:
        bulk_insert_row_number = None
        item_id = None
        for items in prices:
            if items['column_name'] == 'bulk_insert_row_number':
                bulk_insert_row_number = items['value']
                item_id = key_value_products_dict[bulk_insert_row_number]
            if items['column_name'] == 'item_id':
                items['value'] = item_id
    return price_import_data_list


def update_current_stock_on_items(existing_products, update_products ):
    new_update_list = []
    for item in update_products:
        name = item['name']
        name = str(name).lower().strip()
        new_current_stock = item['current_stock']
        new_current_stock = Decimal(new_current_stock or '0')
        if name in existing_products:
            product = existing_products.get(name)
            if product:
                item_id = product['id_item']
                existing_current_stock = product['current_stock']
                existing_current_stock = Decimal(existing_current_stock or '0')
                current_stock = existing_current_stock + new_current_stock
                new_update_list.append((current_stock, item_id))
    update_items_current_stock(tuple(new_update_list))
    return "Successfully updated"
    


 


    
