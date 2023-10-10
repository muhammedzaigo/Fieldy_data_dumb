from database_connection import *
import datetime

CONTACT = 1
ORGAZANAIZATION = 2


def get_bulk_insert_id(context, insert=False):
    created_by = context["created_by"]
    which_user = context["which_user"]
    filename = context["filename"]
    TENANT_ID = context["TENANT_ID"]
    if which_user == 2:
        entity = "organization"
    elif which_user == 1:
        entity = "contact"
    else:
        entity = "products"

    bulk_insert_id: int = 0
    try:
        if insert:
            qry = '''INSERT INTO `bulk_insert`(`created_at`,`original_file_name`,`created_by`,`entity`,`tenant_id`) VALUES (%s,%s,%s,%s,%s)'''
            val = (datetime.datetime.now(), filename,
                   created_by, entity, TENANT_ID)
            bulk_insert_id = insert_update_delete(qry, val)
        # if select:
        #     qry = '''SELECT `id` FROM `bulk_insert` WHERE `tenant_id` = %s ORDER BY id DESC LIMIT 1'''
        #     last_row_id = select_all(qry,TENANT_ID)
        #     bulk_insert_id = last_row_id[0][0]
    except Exception as e:
        print(f"bulk_insert : {str(e)}")
    return bulk_insert_id


def single_insert(name, email, TENANT_ID, time, insert=False):
    return_val: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `customer_group`(`name`,`email`,`TENANT_ID`,`created_at`) VALUES (%s,%s,%s,%s)"
            val = (name, email, TENANT_ID, time)
            return_val = insert_update_delete(qry, val)
    except Exception as e:
        print(f"single insert customer_group : {str(e)}")
    return return_val


def single_delete(email, delete=False):
    try:
        if delete:
            qry = "DELETE FROM `customer_group` WHERE  `email` = %s"
            insert_update_delete(qry, email)
    except Exception as e:
        print(f"single delete customer_group : {str(e)}")
    return "Deleted Successfully"


def bulk_delete_custemer_group_and_addresses(emails, lines, delete_custemer_group=False, delete_addresses=False):
    try:
        if delete_custemer_group:
            qry = "DELETE FROM `customer_group` WHERE  `email` IN (%s) "
            insert_update_delete_many(qry, emails)
        if delete_addresses:
            qry = "DELETE FROM `addresses` WHERE  `line_1` IN (%s)"
            insert_update_delete_many(qry, lines)
    except Exception as e:
        print(f"bulk_delete_custemer_group_and_addresses : {str(e)}")


def bulk_insert_custemer_group(customer_group, bulk_insert_id, insert=False, select=False):
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


def bulk_insert_addresses(address, bulk_insert_id, select=False, insert=False):

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


def bulk_insert_customer_group_addresses(customer_group_addresses, bulk_insert_id, select=False, insert=False):
    customer_group_addresses_all: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `customer_group_addresses`(`tenant_id`,`id_customer_group`,`id_address`,`is_primary`,`created_at`,`bulk_insert_id`) VALUES (%s,%s,%s,%s,%s,%s)"
            insert_update_delete_many(qry, customer_group_addresses)
        if select:
            qry = '''  SELECT `id_customer_group`,`id_address`,`bulk_insert_id` FROM `customer_group_addresses` WHERE `is_primary` = 1 AND `bulk_insert_id`= %s'''
            customer_group_addresses_all = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"customer_group_addresses : {str(e)}")
    return customer_group_addresses_all


def bulk_insert_users(users_data_and_customer_group, coustomer_id, TENANT_ID, select=False, insert=False):
    return_all_list = []
    email_List = []
    types = ""
    try:
        if insert:
            types = "insert"
            qry = "INSERT INTO `users`(`name`,`first_name`,`last_name`,`email`,`phone`,`job_title`,`id_customer_group`,`tenant_id`,`role_id`,`created_by`,`status`,`password`,`created_at`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            insert_update_delete_many(qry, users_data_and_customer_group)
        if select:
            types = "select"
            qry = '''SELECT `first_name`,`last_name`,`email`,`phone`,`job_title`,`id_customer_group` FROM `users` WHERE `tenant_id` = %s AND `id_customer_group` = %s '''
            val = (TENANT_ID, coustomer_id)
            users = select_filter(qry, val)

            for user in users:
                if user[2] != None:
                    if len(str(user[2])) != 0:
                        email_List.append(user[2])
                return_list = []
                for single in user:
                    if single != None:
                        if len(str(single)) == 0:
                            single = None
                    return_list.append(single)
                return_all_list.append(return_list)

    except Exception as e:
        print(f"users {types} : {str(e)} ")
    return {"return_all_list": return_all_list, "email_List": email_List}


def retrive_role_id(TENANT_ID, select=False):
    role = None
    try:
        if select:
            qry = '''SELECT `id` FROM `roles` WHERE `tenant_id` = %s and `name` = %s'''
            role = select_one(qry, (TENANT_ID, "customer"))
            role = role[0]
    except Exception as e:
        print(f"role : {str(e)}")
    return role


def bulk_insert_phones(phone_number_and_customer_group, select=False, insert=False):
    phones: tuple = ()
    try:
        if insert:
            qry = "INSERT INTO `phones`(`number`,`label`,`phoneable_id`,`TENANT_ID`,`phoneable_type`,`raw_number`,`created_at`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            insert_update_delete_many(qry, phone_number_and_customer_group)
        if select:
            qry = '''  SELECT `number`,`phoneable_id`  FROM `phones`'''
            phones = select_all(qry)
    except Exception as e:
        print(f"phones : {str(e)}")
    return phones


def bulk_insert_files(files_db_dump_data, bulk_insert_id, insert=False, select=False):
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


def bulk_update_customer_group(update_file_id_custemer_group, insert=False):
    customer_group_id_and_emails: tuple = ()
    if insert:
        qry = '''INSERT INTO `customer_group`(`id_customer_group`,`files`,`tenant_id`,`bulk_insert_id`,`updated_at`) VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE files = VALUES(files), updated_at = VALUES(updated_at)'''
        customer_group_id_and_emails = insert_update_delete_many(
            qry, update_file_id_custemer_group)
    return customer_group_id_and_emails


def bulk_update_customer_group_using_primary_address(customer_group_using_primary_address, insert=False):
    using_primary_address: tuple = ()
    if insert:
        qry = '''INSERT INTO `customer_group`(`id_customer_group`,`id_address_front`,`bulk_insert_id`,`tenant_id`) VALUES (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE id_address_front = VALUES(id_address_front)'''
        using_primary_address = insert_update_delete_many(
            qry, customer_group_using_primary_address)
    return using_primary_address


def bulk_insert_dynamic(table_name, column_names, values, insert=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if insert:
            make_presentage_s = ",".join(["%s"] * len(values[0]))
            column_names = "({})".format(
                ", ".join("`{}`".format(name) for name in column_names))
            qry = f'''INSERT INTO `{table_name}` {column_names} VALUES ({make_presentage_s})'''
            insert_update_delete_many(qry, values)
    except Exception as e:
        print(f"insert {table_name} : {str(e)}")
    return customer_group_id_and_emails


def retrive_customer_group_and_addresses_data_use_bulk_insert_id(table_name, bulk_insert_id, custemer_type=None, select_customer_group=False, select_address=False):
    customer_group_id_and_emails: tuple = ()
    try:
        if select_customer_group:
            qry = ''' SELECT `id_customer_group`,`email`,`name`,`website`,`row_index` FROM `customer_group` WHERE `bulk_insert_id` = %s and `customer_type` = %s'''
            val = (bulk_insert_id, custemer_type)
            customer_group_id_and_emails = select_filter(qry, val)
        if select_address:
            qry = ''' SELECT `id_address`,`line_1`,`line_2`,`zip_code`,`city`,`state`,`branch_name`,`first_name`,`last_name`,`row_index` FROM `addresses` WHERE `bulk_insert_id` = %s'''
            customer_group_id_and_emails = select_filter(qry, bulk_insert_id)
    except Exception as e:
        print(f"{table_name} : {str(e)}")
    return customer_group_id_and_emails


def get_bulk_retrive_using_tenant_id(context, json_format):
    TENANT_ID = context["TENANT_ID"]
    which_user = context["which_user"]
    retrive_customer_data_using_tenant_id = []
    return_context = {}
    try:
        if which_user == ORGAZANAIZATION:

            if "dupicate_name_in_csv" in context.keys():
                qry = '''SELECT `customer_group`.`id_customer_group`,`customer_group`.`name` FROM `customer_group`WHERE `customer_group`.`tenant_id` = %s AND `customer_group`.`customer_type` = %s '''
                val = (TENANT_ID, "company_customer")
                retrive_customer_data_using_tenant_id = select_filter(qry, val)
                names_List = []
                name_and_id_dict = {}
                for single_list in retrive_customer_data_using_tenant_id:
                    if single_list[1] != None:
                        if len(single_list[1]) != 0:
                            names_List.append(single_list[1])
                            name_and_id_dict.update(
                                {single_list[1]: single_list[0]})
                return_context = {"return_data_List": [], "names_List": list(set(
                    names_List)), "emails_List": [], "all_check_List": [], "name_and_id_dict": name_and_id_dict}
            else:
                qry = '''SELECT `customer_group`.`id_customer_group`, `customer_group`.`name`, `users`.`first_name`, `users`.`last_name`, `users`.`email`, `users`.`phone`, `users`.`job_title`, `phones`.`number`, `phones`.`raw_number` FROM `customer_group`
                LEFT JOIN `users` ON `users`.`id_customer_group`=`customer_group`.`id_customer_group` LEFT JOIN `phones` ON `phones`.`phoneable_id`=`customer_group`.`id_customer_group` 
                WHERE `users`.`tenant_id`= %s AND `customer_group`.`tenant_id` = %s AND `customer_group`.`customer_type` = %s'''
                val = (TENANT_ID, TENANT_ID, "company_customer")
                retrive_customer_data_using_tenant_id = select_filter(qry, val)

                return_data_List = []
                names_List = []
                all_check_List = []
                emails_List = []
                name_and_id_dict = {}
                for single_list in retrive_customer_data_using_tenant_id:
                    if single_list[1] != None:
                        if len(single_list[1]) != 0:
                            names_List.append(single_list[1])
                            name_and_id_dict.update(
                                {single_list[1]: single_list[0]})

                    if single_list[4] != None:
                        if len(single_list[4]) != 0:
                            emails_List.append(single_list[4])

                    return_data = []
                    for single in single_list:
                        if single != None:
                            if len(str(single)) == 0:
                                single = None
                        return_data.append(single)

                    check_list = [return_data[2], return_data[3], return_data[4],
                                  return_data[5], return_data[6], return_data[7], return_data[8]]
                    all_check_List.append(check_list)
                    return_data_List.append(return_data)
                return_context = {"return_data_List": return_data_List, "names_List": list(set(names_List)), "emails_List": list(
                    set(emails_List)), "all_check_List": all_check_List, "name_and_id_dict": name_and_id_dict}

        if which_user == CONTACT:
            qry = '''SELECT `users`.`first_name`, `users`.`last_name`, `users`.`email`, `phones`.`number`, `phones`.`raw_number` FROM `customer_group`
            LEFT JOIN `users` ON `users`.`id_customer_group`=`customer_group`.`id_customer_group` LEFT JOIN `phones` ON `phones`.`phoneable_id`=`customer_group`.`id_customer_group` 
            WHERE `users`.`tenant_id`= %s AND `customer_group`.`tenant_id` = %s AND `customer_group`.`customer_type` = %s'''
            val = (TENANT_ID, TENANT_ID, "contact_customer")

            retrive_customer_data_using_tenant_id = select_filter(qry, val)
            return_data_List = []
            emails = []
            for single_list in retrive_customer_data_using_tenant_id:
                if single_list[2] != None:
                    if len(single_list[2]) != 0:
                        emails.append(single_list[2])
                return_data = []
                for single in single_list:
                    if single != None:
                        if len(single) == 0:
                            single = None
                    return_data.append(single)
                return_data_List.append(return_data)
            return_context = {
                "return_data_List": return_data_List, "emails": emails}
    except Exception as e:
        print(f"get_bulk_retrive_using_tenant_id : {str(e)}")
    return return_context


## product query

def retrive_products_by_tenant(tenant):
    product_names = []
    try:
        qry = '''SELECT `name` FROM `items` WHERE `id_tenant` = %s'''
        existing_products = select_filter(qry, (tenant))
        product_names = []
        for name in existing_products:
            try:
                name = str(name[0]).lower().strip()
                product_names.append(name)
            except:
                pass
        product_names = list(set(product_names))
    except Exception as e:
        print(f"product : {str(e)}")
    return product_names


def retrive_products_use_bulk_insert_id(bulk_insert_id):
    products : tuple = ()
    try:
        qry = ''' SELECT `id_item`, `bulk_insert_row_number`  FROM `items` WHERE `bulk_insert_id` = %s'''
        val = (bulk_insert_id)
        products = select_filter(qry, val)
    except Exception as e:
        print(f"items table : {str(e)}")
    return products