import random
import string
import datetime
import avinit
import bcrypt
import threading
import os
from utils.query import *

UPLOAD_FOLDER = "media"
SALT = b'$2y$10$/XihfLhBx5RphDLAxfldkOdyEy6seEfWuA1oGGkfNYslabtmYndT'
DEFAULT_PASSWORD = b'Fieldy@123'
FIELDY_AT_123 = "$2y$10$/XihfLhBx5RphDLAxfldkOyPDO4YAv9YaGPtzmN/LvUpUTCkdlA82"
TENANT_ID = 15
MIME = "image/png"
SECRET_KEY = "f#6=zf!2=n@ed-=6g17&k4e4fl#d4v&l*v6q5_6=8jz1f98v#"

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
        hashed_password = FIELDY_AT_123
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

