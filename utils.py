import random
import string
import datetime
import avinit
import bcrypt

UPLOAD_FOLDER = "media"
SALT = b'$2y$10$/XihfLhBx5RphDLAxfldkOdyEy6seEfWuA1oGGkfNYslabtmYndT'
DEFAULT_PASSWORD = b'Fieldy@123'

def create_avatar(name,create=False):
    if create :
        file_name = "MEM_" + name + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        avinit.get_png_avatar(name, output_file=f'{UPLOAD_FOLDER}/{file_name}.png')
        text_file = open(f"{UPLOAD_FOLDER}/{file_name}.png", 'rb')
        return text_file.name

def random_string(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def password_hash(password):
    hashed_password = ""
    try:
        hashed_password = bcrypt.hashpw(password,SALT)
        hashed_password =  hashed_password.decode()
    except Exception as e:
        print(str(e))
    return hashed_password