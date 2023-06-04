import pandas as pd
import sqlite3
from PyPDF2 import PdfReader
import docx2txt

def connect_db(path):
    connect = sqlite3.connect(path)
    return connect.cursor()


def read_file(file_path):
    return pd.read_excel(file_path)


def create_table(cursor, create_table_command):
    cursor.execute(create_table_command)


def insert_data(cur,insert_command,insert_data):
    cur.execute(insert_command, insert_data)


def print_table(cur, table_name):
    cur.execute(f"SELECT * FROM {table_name}")
    tables = cur.fetchall()
    for row in tables:
        print(row)


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
    return text

