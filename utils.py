import random
import string
import datetime
import avinit


UPLOAD_FOLDER = "media"

def create_avatar(name):
    file_name = "MEM_" + name + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    avinit.get_png_avatar(name, output_file=f'{UPLOAD_FOLDER}/{file_name}.png')
    text_file = open(f"{UPLOAD_FOLDER}/{file_name}.png", 'rb')
    return text_file.name

def random_string(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str