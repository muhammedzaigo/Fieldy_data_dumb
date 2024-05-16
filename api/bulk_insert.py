from utils.query import *
from utils.utils import *
import traceback

from flask import *
product_bulk_upload = Blueprint('product_bulk_upload', __name__)

row = {}
tenent_id = ""

query_item = '''INSERT INTO `items` (`name`,`description`,`sku`,`hsn`,`id_tenant`,`current_stock`,`low_stock`,`is_product`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
val = (row["Product Name"], row["Description"],row['SKU'],row['HSN'],str(tenent_id),row['Available Quantity'],row['Low Stock Threshold'],1)
new_id = insert_update_delete(query_item, val)
query_price = '''INSERT INTO `item_prices` (`item_id`,`price`) VALUES (%s,%s)'''
val_price = (new_id, row['Price'])
insert_update_delete(query_price, val_price)
