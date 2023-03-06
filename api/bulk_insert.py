from utils.json_condition import *
from utils.utils import (password_hash, TENANT_ID, SECRET_KEY, DEFAULT_PASSWORD,
                         create_avatar_then_dumb_files_db_and_map_customer_group_thread)
import io
from flask import *
import csv
import datetime
import threading
from utils.query import *

app = Flask(__name__)
app.secret_key = SECRET_KEY


@app.route("/api/customer_group", methods=['POST'])
def customer_group():
    start = datetime.datetime.now()

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})

    file = request.files['file']
    import_sheet = file.read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(import_sheet))
    bulk_insert_id = get_bulk_insert_id(select=True)
    customer_group = []
    address = []
    emails = []
    lines = []

    for line in reader:
        first_name = line.get('first_name')
        last_name = line.get('last_name')
        email = line.get('email')
        address1 = line.get('address1')
        address2 = line.get('address2')
        name = first_name+" "+last_name
        time = datetime.datetime.now()
        customer_group.append((name, email, TENANT_ID, time, bulk_insert_id))
        address.append((TENANT_ID, address1, address2, time, bulk_insert_id))

        # emails.append(email)
        # lines.append(address1)

        # single_insert_responce = single_insert(name, email, TENANT_ID, time)
        # single_delete_responce = single_delete(email)

    # bulk_delete_responce = bulk_delete_custemer_group_and_addresses(emails,lines)

    customer_group_id_and_emails = bulk_insert_custemer_group(
        customer_group, bulk_insert_id, insert=True, select=True)
    address_id_and_lines = bulk_insert_addresses(
        address, bulk_insert_id, insert=True, select=True)

    create_avatar_then_dumb_files_db_and_map_customer_group = threading.Thread(
        target=create_avatar_then_dumb_files_db_and_map_customer_group_thread, args=(customer_group_id_and_emails, bulk_insert_id,))
    create_avatar_then_dumb_files_db_and_map_customer_group.start()

    # ---------------------------------------------------------------- Map a customer_group_pk and address_pk ------------------------------------------------------------------

    customer_group_addresses = []  # customer_group_pk and address_pk
    phone_number_and_customer_group = []
    users_data_and_customer_group = []
    id_address = []

    hash_password = password_hash(DEFAULT_PASSWORD)
    reader = csv.DictReader(io.StringIO(import_sheet))
    for line in reader:
        email = line.get('email')
        address1 = line.get('address1')
        address2 = line.get('address2')
        phone = line.get('phone')
        first_name = line.get('first_name')
        last_name = line.get('last_name')
        name = first_name+" "+last_name

        customer_group_pk_and_address_pk = []

        for customer_group_id_and_email in customer_group_id_and_emails:
            if email == customer_group_id_and_email[1] and name == customer_group_id_and_email[2]:
                # map customer_group_pk and phone number for phones table
                phone_number_and_customer_group.append(
                    (phone, customer_group_id_and_email[0], TENANT_ID, datetime.datetime.now()))
                # map customer_group_pk and first name and last name for users table
                users_data_and_customer_group.append(
                    (name, first_name, last_name, email, customer_group_id_and_email[0], hash_password, datetime.datetime.now()))

                customer_group_pk_and_address_pk.append(
                    customer_group_id_and_email[0])
                break

        for address_id_and_line in address_id_and_lines:
            if address1 == address_id_and_line[1] and address2 == address_id_and_line[2]:

                address_id = address_id_and_line[0]
                customer_group_pk_and_address_pk.append(address_id)
                id_address.append(address_id)
                break

        customer_group_pk_and_address_pk.insert(0, TENANT_ID)
        customer_group_pk_and_address_pk.insert(3, datetime.datetime.now())
        customer_group_addresses.append(
            tuple(customer_group_pk_and_address_pk))

    customer_group_addresses_all = bulk_insert_customer_group_addresses(customer_group_addresses, id_address, insert=True)
    users = bulk_insert_users(users_data_and_customer_group, insert=True)
    phones = bulk_insert_phones(phone_number_and_customer_group, insert=True)

    data = {
        "diffence":  str(datetime.datetime.now() - start),
        'message': 'File uploaded successfully',
    }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response
