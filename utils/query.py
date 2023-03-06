from database_connection import *
import datetime


def get_bulk_insert_id(insert=False,select=False):
    bulk_insert_id: int = 0
    bulk_insert_number = 1
    try:
        if insert:
            qry = '''INSERT INTO `bulk_insert`(`bulk_insert_number`,`created_at`) VALUES (%s,%s)'''
            val = (bulk_insert_number, datetime.datetime.now())
            last_row_id = insert_update_delete(qry,val)
            
        if select:
            qry = '''SELECT `id` FROM `bulk_insert` ORDER BY id DESC LIMIT 1'''
            last_row_id = select_all(qry)
            bulk_insert_id = last_row_id[0][0]
    except Exception as e:
        print(f"bulk_insert : {str(e)}")
    return bulk_insert_id


def single_insert(name, email, TENANT_ID, time,insert=False):
    return_val : tuple = ()
    try:
        if insert :
            qry = "INSERT INTO `customer_group`(`name`,`email`,`TENANT_ID`,`created_at`) VALUES (%s,%s,%s,%s)"
            val = (name, email, TENANT_ID, time)
            return_val = insert_update_delete(qry, val)
    except Exception as e:
        print(f"single insert customer_group : {str(e)}")
    return return_val


def single_delete(email,delete=False):
    try:
        if delete:
            qry = "DELETE FROM `customer_group` WHERE  `email` = %s"
            insert_update_delete(qry, email)
    except Exception as e:
        print(f"single delete customer_group : {str(e)}")
    return "Deleted Successfully"


def bulk_delete_custemer_group_and_addresses(emails,lines,delete_custemer_group=False,delete_addresses=False):
    try:
        if delete_custemer_group:
            qry = "DELETE FROM `customer_group` WHERE  `email` IN (%s) "
            insert_update_delete_many(qry, emails)
        if delete_addresses:
            qry = "DELETE FROM `addresses` WHERE  `line_1` IN (%s)"
            insert_update_delete_many(qry, lines)
    except Exception as e:
        print(f"bulk_delete_custemer_group_and_addresses : {str(e)}")


def bulk_insert_custemer_group(customer_group,bulk_insert_id,insert=False,select=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if insert:
            qry = '''INSERT INTO `customer_group`(`name`,`email`,`TENANT_ID`,`created_at`,`bulk_insert_id`) VALUES (%s,%s,%s,%s,%s)'''
            insert_update_delete_many(qry, customer_group)
        if select:
            qry = ''' SELECT `id_customer_group`,`email`,`name` FROM `customer_group` WHERE `bulk_insert_id` = %s'''
            customer_group_id_and_emails = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"customer_group : {str(e)}")
    return customer_group_id_and_emails


def bulk_insert_addresses(address,bulk_insert_id,select=False,insert=False):

    address_id_and_lines: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `addresses`(`id_tenant`,`line_1`,`line_2`,`created_at`,`bulk_insert_id`) VALUES (%s,%s,%s,%s,%s)"
            insert_update_delete_many(qry, address)
        if select:
            qry = ''' SELECT `id_address`,`line_1`,`line_2` FROM `addresses` WHERE `bulk_insert_id` = %s'''
            address_id_and_lines = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"address : {str(e)}")
    return address_id_and_lines


def bulk_insert_customer_group_addresses(customer_group_addresses,id_address,select=False,delete=False,insert=False):
    customer_group_addresses_all: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `customer_group_addresses`(`tenant_id`,`id_customer_group`,`id_address`,`created_at`) VALUES (%s,%s,%s,%s)"
            insert_update_delete_many(qry, customer_group_addresses)
        if delete:
            qry = "DELETE FROM `customer_group_addresses` WHERE `id_address` IN (%s) "
            insert_update_delete_many(qry, id_address)
        if select:
            qry = '''  SELECT * FROM `customer_group_addresses`'''
            customer_group_addresses_all = select_all(qry)
    except Exception as e:
        print(f"customer_group_addresses : {str(e)}")
    return customer_group_addresses_all


def bulk_insert_users(users_data_and_customer_group,select=False,insert=False):
    users: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `users`(`name`,`first_name`,`last_name`,`email`,`id_customer_group`,`password`,`created_at`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            insert_update_delete_many(qry, users_data_and_customer_group)
        if select:
            qry = '''SELECT `email`,`id_customer_group` FROM `users`'''
            users = select_all(qry)
    except Exception as e:
        print(f"users : {str(e)}")
    return users


def bulk_insert_phones(phone_number_and_customer_group,select=False,insert=False):
    phones: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `phones`(`number`,`phoneable_id`,`TENANT_ID`,`created_at`) VALUES (%s,%s,%s,%s)"
            insert_update_delete_many(qry, phone_number_and_customer_group)
        if select:
            qry = '''  SELECT `number`,`phoneable_id`  FROM `phones`'''
            phones = select_all(qry)
    except Exception as e:
        print(f"phones : {str(e)}")
    return phones


def bulk_insert_files(files_db_dump_data,bulk_insert_id,insert=False,select=False):
    files_list: tuple = ()
    try:
        if insert:
            qry = '''INSERT INTO `files`(`id_tenant`,`mime`,`file_name`,`identifier`,`created_at`,`bulk_insert_id`) VALUES (%s,%s,%s,%s,%s,%s)'''
            insert_update_delete_many(qry, files_db_dump_data)
        if select:
            qry = ''' SELECT `id_file`,`identifier` FROM `files` WHERE `bulk_insert_id` = %s'''
            files_list = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"files : {str(e)}")
    return files_list


def bulk_update_customer_group(update_file_id_custemer_group,insert=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if insert:
            qry = '''INSERT INTO `customer_group`(`id_customer_group`,`files`,`tenant_id`,`updated_at`) VALUES (%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE files = VALUES(files), updated_at = VALUES(updated_at)'''
            insert_update_delete_many(qry, update_file_id_custemer_group)
    except Exception as e:
        print(f"update_customer_group : {str(e)}")
    return customer_group_id_and_emails