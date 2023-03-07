import os
import pandas as pd
import csv
import chardet
from flask import *

xlsx_convert_csv = Blueprint('xlsx_convert_csv', __name__)

SHEET_FOLDER = "sheets"
XLSX_FILE_NAME = "123.xlsx"

@xlsx_convert_csv.route("/xlsx_convert_csv", methods=["POST"])
def xlsx_to_csv_api():
    if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'})

    file = request.files['file']
    workbook = pd.ExcelFile(file)
    convert = request.form['convert']
    read = request.form['read']
    
    sheet_names = []
    for sheet_name in workbook.sheet_names:
        try:
            if convert == "True":
                sheet_data = workbook.parse(sheet_name)
                if not os.path.exists(SHEET_FOLDER):
                    os.mkdir(SHEET_FOLDER)
                output_file = f"{SHEET_FOLDER}/{sheet_name}.csv"
                sheet_data.to_csv(output_file, index=False)
                
            sheet_names.append(sheet_name)
        except Exception as e:
            print(str(e)+" problem file --> " +sheet_name)
    
    if read == "True":
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

    data = {
        'message': 'File sparate successfully',
    }
    response = make_response(jsonify(data), 200)
    response.headers["Content-Type"] = "application/json"
    return response
