import random
import string
import datetime
import avinit
import bcrypt
import threading
import os
import pandas as pd
import csv
import chardet

from database_connection import insert_update_delete_many, select_filter

UPLOAD_FOLDER = "media"
SALT = b'$2y$10$/XihfLhBx5RphDLAxfldkOdyEy6seEfWuA1oGGkfNYslabtmYndT'
DEFAULT_PASSWORD = b'Fieldy@123'
TENANT_ID = 15
MIME = "image/png"


def create_avatar(names,create=False):
    if create :
        if not os.path.exists(UPLOAD_FOLDER):
            os.mkdir(UPLOAD_FOLDER)
        for name in names:
            avinit.get_png_avatar(name[0], output_file=f'{UPLOAD_FOLDER}/{name[1]}')
        return "avatars created"


def random_string(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def password_hash(password):
    try:
        hashed_password = bcrypt.hashpw(password,SALT)
        hashed_password =  hashed_password.decode()
    except Exception as e:
        hashed_password = "$2y$10$/XihfLhBx5RphDLAxfldkOyPDO4YAv9YaGPtzmN/LvUpUTCkdlA82"
        print(str(e))
    return hashed_password


def create_avatar_then_dumb_files_db_and_map_customer_group_thread(customer_group_id_and_emails : tuple = (),bulk_insert_id : int = 1): #(`id_customer_group`,`email`,`name`)
    files_db_dump_data = []
    files_identifier_list = []
    create_avatar_names = []
    
    for index, fields in enumerate(customer_group_id_and_emails,1):
        name = f"{fields[2]}_{index}"
        file_name = f"{name}__{TENANT_ID}_{bulk_insert_id}_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name_with_ext = f"{file_name}.png"
        
        create_avatar_names.append((name,file_name_with_ext))
        files_db_dump_data.append((TENANT_ID,MIME,file_name_with_ext,file_name_with_ext,datetime.datetime.now(),bulk_insert_id))
        files_identifier_list.append((file_name_with_ext,fields[0])) # file_identifier and customer_group_id

    create_avatar_thread = threading.Thread(target=create_avatar,args=(create_avatar_names,True,))
    create_avatar_thread.start()
    
    bulk_insert_files_list = bulk_insert_files(files_db_dump_data,bulk_insert_id,insert=True,select=True)
    
    update_file_id_custemer_group = []
    
    for file in bulk_insert_files_list:
        file_id = file[0]
        file_identifier = file[1]
        
        for file_items in files_identifier_list:
            if file_items[0] == file_identifier: # file_identifier == file_identifier
                update_file_id_custemer_group.append((file_items[1],file_id,TENANT_ID,datetime.datetime.now())) #`id_customer_group_id`,`id_file`
                continue
    bulk_update_customer_with_file = bulk_update_customer_group(update_file_id_custemer_group,insert=True)
    return 


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



SHEET_FOLDER = "sheets"
XLSX_FILE_NAME = "123.xlsx"


def xlsx_to_csv(convert = False,read = False):

    workbook = pd.ExcelFile(XLSX_FILE_NAME)
    sheet_names = []
    for sheet_name in workbook.sheet_names:
        try:
            if convert:
                sheet_data = workbook.parse(sheet_name)
                if not os.path.exists(SHEET_FOLDER):
                    os.mkdir(SHEET_FOLDER)
                output_file = f"{SHEET_FOLDER}/{sheet_name}.csv"
                sheet_data.to_csv(output_file, index=False)
                
            sheet_names.append(sheet_name)
        except Exception as e:
            print(str(e)+" problem file --> " +sheet_name)
    
    if read:
        for index,sheet_name in enumerate( sheet_names,1):
            try:
                with open(f"{SHEET_FOLDER}/{sheet_name}.csv", "r") as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        print(f"index - {index} sheet_name - {sheet_name}")
                        
            except Exception as e:
                print(str(e)+" problem file --> " +sheet_name)
                
                with open(f"{SHEET_FOLDER}/{sheet_name}.csv", 'rb') as csv_file:
                    encoding_type = chardet.detect(csv_file.read())
                    
                with open(f"{SHEET_FOLDER}/{sheet_name}.csv", "r",encoding=encoding_type['encoding']) as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        print(f"index - {index} sheet_name - {sheet_name}")



