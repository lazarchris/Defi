import DataFeeder
import os

"""

Exercice 1: Feed DWH_PATIENT &  DWH_PATIENT_IPPHIST Tables  

"""

# Load and read excel file
df = DataFeeder.read_file(r"data/source/export_patient.xlsx")

# DB connection
cur = DataFeeder.connect_db("data/drwh.db")

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
    DataFeeder.insert_data(
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
    DataFeeder.insert_data(cur, sql_command, (patient_num, hospital_patient_id))


"""
Exercise 2: Feed DWH_DOCUMENT table

"""

reports_pdf = os.listdir("data/reports/pdfs")
report_docx = os.listdir("data/reports/docx")

# Insertion of pdf data into table HOSPITAL_PATIENT
for doc in reports_pdf:
    doc_infos = DataFeeder.extract_ids(doc)
    doc_text = DataFeeder.extract_pdf(doc)
    sql_command = "INSERT INTO DWH_DOCUMENT (DOCUMENT_NUM, DOCUMENT_TYPE, PATIENT_NUM, DISPLAYED_TEXT) VALUES ( ?, ? ,? , ? )"
    DataFeeder.insert_data(cur, sql_command, (doc_infos[0], doc_infos[1], doc_infos[2], doc_text))


# Insertion of docx data into table HOSPITAL_PATIENT
for doc in report_docx:
    doc_infos = DataFeeder.extract_ids(doc)
    doc_text = DataFeeder.extract_docx(doc)
    sql_command = "INSERT INTO DWH_DOCUMENT (DOCUMENT_NUM, DOCUMENT_TYPE, PATIENT_NUM, DISPLAYED_TEXT) VALUES ( ?, ? ,? , ? )"
    DataFeeder.insert_data(cur, sql_command, (doc_infos[0], doc_infos[1], doc_infos[2], doc_text))

"""
Results
"""
DataFeeder.print_table(cur, "DWH_PATIENT")
DataFeeder.print_table(cur, "DWH_PATIENT_IPPHIST")
DataFeeder.print_table(cur, "DWH_DOCUMENT")
