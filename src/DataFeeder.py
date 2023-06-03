import pandas as pd
import sqlite3
from PyPDF2 import PdfReader
import os
import docx2txt


"""

Exercice 1: Feed DWH_PATIENT &  DWH_PATIENT_IPPHIST Tables  

"""

def read_file(file_path):
    return pd.read_excel(file_path)

def connect_db(path):
    connect = sqlite3.connect(path)
    return connect.cursor()


def create_table(cursor, create_table_command):
    cursor.execute(create_table_command)


def insert_data(
    cur,
    insert_command,
    insert_data,
):
    cur.execute(insert_command, insert_data)

def print_table(cur,table_name):
    cur.execute(f"SELECT * FROM {table_name}")
    tables = cur.fetchall()
    for row in tables:
        print(row)   

# Load and read excel file
df = read_file(r"data/source/export_patient.xlsx")

# DB connection
cur = connect_db("data/drwh.db")

# Feed DWH_PATIENT able
for index, row in df.iterrows():
    patient_num = row["HOSPITAL_PATIENT_ID"]
    last_name = row["NOM"]
    first_name = row["PRENOM"]
    birth_date = row["DATE_NAISSANCE"]
    sex = row["SEXE"]
    maiden_name = row["NOM_JEUNE_FILLE"]
    residence_address = row["ADRESSE"]
    phone_number = row["TEL"]
    zip_code = row["CP"]
    residence_city = row["VILLE"]
    country = row["PAYS"]
    date_mort = row["DATE_MORT"]

    sql_command = "INSERT INTO DWH_PATIENT (PATIENT_NUM, LASTNAME, FIRSTNAME, BIRTH_DATE, SEX, MAIDEN_NAME ,RESIDENCE_ADDRESS, PHONE_NUMBER, ZIP_CODE, RESIDENCE_CITY, DEATH_DATE, RESIDENCE_COUNTRY) VALUES ( ?, ?, ?,?,?, ?, ?, ? ,?, ?, ?, ? )"
    insert_data(
        cur,
        sql_command,
        (
            patient_num,
            last_name,
            first_name,
            birth_date,
            sex,
            maiden_name,
            residence_address,
            phone_number,
            zip_code,
            residence_city,
            country,
            date_mort,
        ),
    )

# feed data into DWH_PATIENT_IPPHIST table
for index, row in df.iterrows():

    patient_num = row["HOSPITAL_PATIENT_ID"]
    hospital_patient_id = row["HOSPITAL_PATIENT_ID"]

    sql_command = "INSERT INTO DWH_PATIENT_IPPHIST (PATIENT_NUM, HOSPITAL_PATIENT_ID) VALUES ( ?, ? )"
    insert_data(cur, sql_command, (patient_num, hospital_patient_id))



"""
Exercise 2: Feed DWH_DOCUMENT table

"""
def extract_ids(doc_name):
    split_var = doc_name.split("_")
    patient_id = split_var[0]
    doc_id = split_var[1].split(".")[0]
    doc_type = split_var[1].split(".")[1]
    return (doc_id, doc_type, patient_id)

def extract_pdf(doc_name):
    reader = PdfReader(f"data/reports/pdfs/{doc_name}")
    text = ""
    for page in reader.pages:
        text += page.extract_text()
    return text

def extract_docx(doc_name):
    text = docx2txt.process(f"data/reports/docx/{doc_name}")
    print(text)


reports_pdf = os.listdir("data/reports/pdfs")
report_docx = os.listdir("data/reports/docx")

 # Insertion of pdf data into table HOSPITAL_PATIENT
for doc in reports_pdf:
    doc_infos = extract_ids(doc)
    doc_text = extract_pdf(doc)
    sql_command = "INSERT INTO DWH_DOCUMENT (DOCUMENT_NUM, DOCUMENT_TYPE, PATIENT_NUM, DISPLAYED_TEXT) VALUES ( ?, ? ,? , ? )"
    insert_data(cur, sql_command, (doc_infos[0], doc_infos[1], doc_infos[2], doc_text))


 # Insertion of docx data into table HOSPITAL_PATIENT
for doc in report_docx:
    doc_infos = extract_ids(doc)
    doc_text = extract_docx(doc)
    sql_command = "INSERT INTO DWH_DOCUMENT (DOCUMENT_NUM, DOCUMENT_TYPE, PATIENT_NUM, DISPLAYED_TEXT) VALUES ( ?, ? ,? , ? )"
    insert_data(cur, sql_command, (doc_infos[0], doc_infos[1], doc_infos[2], doc_text))


"""
Results
"""
print_table(cur,"DWH_PATIENT")
print_table(cur,"DWH_PATIENT_IPPHIST")
print_table(cur,"DWH_DOCUMENT")
