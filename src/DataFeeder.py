import pandas as pd
import sqlite3
from PyPDF2 import PdfReader
import os
import docx2txt
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

# Table creation
sql_command = """
CREATE TABLE IF NOT EXISTS DWH_PATIENT
(
PATIENT_NUM INTEGER PRIMARY KEY,
LASTNAME VARCHAR2(100),
FIRSTNAME VARCHAR2(40),
BIRTH_DATE DATE,
SEX VARCHAR2(2),
MAIDEN_NAME VARCHAR2(81),
RESIDENCE_ADDRESS VARCHAR2(1000),
PHONE_NUMBER VARCHAR2(1000),
ZIP_CODE VARCHAR2(30),
RESIDENCE_CITY VARCHAR2(200),
DEATH_DATE DATE,
RESIDENCE_COUNTRY VARCHAR2(100),
RESIDENCE_LATITUDE VARCHAR2(300),
RESIDENCE_LONGITUDE VARCHAR2(300),
DEATH_CODE VARCHAR2(2),UPDATE_DATE DATE,
BIRTH_COUNTRY VARCHAR2(100),
BIRTH_CITY VARCHAR2(100),
BIRTH_ZIP_CODE VARCHAR2(10),
BIRTH_LATITUDE FLOAT(126),
BIRTH_LONGITUDE FLOAT(126),
UPLOAD_ID INTEGER
)
"""
create_table(cur, sql_command)


# Feed database
colomns = [
    "NOM",
    "PRENOM",
    "DATE_NAISSANCE",
    "SEXE",
    "NOM_JEUNE_FILLE",
    "HOSPITAL_PATIENT_ID",
    "ADRESSE",
    "TEL",
    "CP",
    "VILLE",
    "PAYS",
    "DATE_MORT",
]

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


# second Table creation
sql_command = """
CREATE TABLE IF NOT EXISTS DWH_PATIENT_IPPHIST
(
PATIENT_NUM INTEGER,
HOSPITAL_PATIENT_ID VARCHAR2(100),
ORIGIN_PATIENT_ID VARCHAR2(40),
MASTER_PATIENT_ID INTEGER,
UPLOAD_ID INTEGER
)
"""
create_table(cur, sql_command)

# feed data into DWH_PATIENT_IPPHIST table
for index, row in df.iterrows():

    patient_num = row["HOSPITAL_PATIENT_ID"]
    hospital_patient_id = row["HOSPITAL_PATIENT_ID"]

    sql_command = "INSERT INTO DWH_PATIENT_IPPHIST (PATIENT_NUM, HOSPITAL_PATIENT_ID) VALUES ( ?, ? )"
    insert_data(cur, sql_command, (patient_num, hospital_patient_id))

# Exercise 2

sql_command = """
CREATE TABLE IF NOT EXISTS DWH_DOCUMENT
(
DOCUMENT_NUM INTEGER NOT NULL,
PATIENT_NUM INTEGER,
ENCOUNTER_NUM VARCHAR2(30),
TITLE VARCHAR2(400),
DOCUMENT_ORIGIN_CODE VARCHAR2(40),
DOCUMENT_DATE DATE,
ID_DOC_SOURCE VARCHAR2(300),
DOCUMENT_TYPE VARCHAR2(40),
DISPLAYED_TEXT CLOB,
AUTHOR VARCHAR2(200),
UNIT_CODE VARCHAR2(30),
UNIT_NUM INTEGER,
DEPARTMENT_NUM INTEGER,
EXTRACTCONTEXT_DONE_FLAG INTEGER,
EXTRACTCONCEPT_DONE_FLAG INTEGER,
ENRGENE_DONE_FLAG INTEGER,
ENRICHTEXT_DONE_FLAG INTEGER,
UPDATE_DATE DATE,
UPLOAD_ID INTEGER,
PRIMARY KEY (DOCUMENT_NUM)
)
"""
create_table(cur, sql_command)

#########################
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

# print_table(cur,"DWH_DOCUMENT")
