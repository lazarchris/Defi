import pandas as pd
import sqlite3

def read_file(file_path):
    return pd.read_excel(file_path)


def connect_db(path):
    connect = sqlite3.connect(path)
    return connect.cursor()

def create_table(cursor,create_table_command):
    cursor.execute(create_table_command)

def insert_data(cur,  insert_command,insert_data,):
    cur.execute(insert_command, insert_data)
    
# Load and read excel file
df = read_file(r'data/source/export_patient.xlsx')


# DB connection
cur = connect_db("data/drwh.db")



# Table creation
sql_command = '''
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
'''
create_table(cur, sql_command)




# Fetch all the table names
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()
print(tables)
# # Print the table names
# for table in tables:
#     print(table[0])
# print("welcome to the data world")


# Feed database
colomns =["NOM","PRENOM","DATE_NAISSANCE","SEXE","NOM_JEUNE_FILLE","HOSPITAL_PATIENT_ID","ADRESSE","TEL","CP","VILLE","PAYS","DATE_MORT"]
for index, row in df.iterrows():
    patient_num = row["HOSPITAL_PATIENT_ID"]
    last_name = row["NOM"]
    first_name = row["PRENOM"]
    birth_date  = row["DATE_NAISSANCE"]
    sex = row["SEXE"]
    maiden_name = row["NOM_JEUNE_FILLE"]
    residence_address = row["ADRESSE"]
    phone_number = row["TEL"]
    zip_code = row["CP"]
    residence_city = row["VILLE"]
    country = row["PAYS"]
    date_mort = row["DATE_MORT"]

    sql_command = "INSERT INTO DWH_PATIENT (PATIENT_NUM, LASTNAME, FIRSTNAME, BIRTH_DATE, SEX, MAIDEN_NAME ,RESIDENCE_ADDRESS, PHONE_NUMBER, ZIP_CODE, RESIDENCE_CITY, DEATH_DATE, RESIDENCE_COUNTRY) VALUES ( ?, ?, ?,?,?, ?, ?, ? ,?, ?, ?, ? )"
    insert_data(cur,sql_command,(patient_num, last_name,first_name, birth_date, sex, maiden_name, residence_address, phone_number, zip_code, residence_city, country, date_mort))



# Fetch all the table names
cur.execute("SELECT * FROM DWH_PATIENT")
tables = cur.fetchall()

# Print the table names
print(tables[0])



###
# CREATE TABLE DWH_PATIENT_IPPHIST (
# PATIENT_NUM INTEGER,
# HOSPITAL_PATIENT_ID VARCHAR2(100),
# ORIGIN_PATIENT_ID VARCHAR2(40),
# MASTER_PATIENT_ID INTEGER,
# UPLOAD_ID INTEGER
# );

# DB connection
#cur = connect_db("data/drwh.db")



# second Table creation
sql_command = '''
CREATE TABLE IF NOT EXISTS DWH_PATIENT_IPPHIST
(
PATIENT_NUM INTEGER,
HOSPITAL_PATIENT_ID VARCHAR2(100),
ORIGIN_PATIENT_ID VARCHAR2(40),
MASTER_PATIENT_ID INTEGER,
UPLOAD_ID INTEGER
)
'''
create_table(cur, sql_command)

#feed data into second table
for index, row in df.iterrows():

    patient_num = row["HOSPITAL_PATIENT_ID"]
    hospital_patient_id = row["HOSPITAL_PATIENT_ID"]



    sql_command = "INSERT INTO DWH_PATIENT_IPPHIST (PATIENT_NUM, HOSPITAL_PATIENT_ID) VALUES ( ?, ? )"
    insert_data(cur,sql_command,(patient_num, hospital_patient_id))




