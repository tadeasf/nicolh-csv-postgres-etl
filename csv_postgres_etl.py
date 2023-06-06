# etl_script.py

import time
import json
import pandas as pd
from paramiko import Transport, SFTPClient
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy import inspect
import io
import csv

# Import your models and config
from models import Base, CsvData
from config import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    SFTP_HOST,
    SFTP_USER,
    SFTP_PASSWORD,
)
from pandas_transformation import (
    simple_transform,
    validate_dataframe,
)

# Connect to the PostgreSQL database
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Set up the SFTP connection
SFTP_PORT = 22
transport = Transport((SFTP_HOST, SFTP_PORT))
transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

sftp = SFTPClient.from_transport(transport)

# The directory to watch for new CSV files
watch_dir = "/home/tadeas/nicolh/csvs"


def save_error_log(error, filename):
    error_log = {
        "file": filename,
        "error": str(error),
    }
    with open("error_log.json", "a") as f:
        json.dump(error_log, f)
        f.write("\n")


def process_dataframe(df, filename):
    # Create the session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Check if table exists, create if not
    inspector = inspect(engine)
    if not inspector.has_table(CsvData.__tablename__):
        Base.metadata.create_all(engine)

    # Apply data transformations to the DataFrame
    df = simple_transform(df)

    # Validate DataFrame against the model
    df = validate_dataframe(df)

    # Append data from the DataFrame to the table
    try:
        # Append data to the table
        df.to_sql(CsvData.__tablename__, engine, if_exists="append", index=False)
    except Exception as e:
        print(f"An error occurred when processing file {filename}: {e}")
        save_error_log(e, filename)
        # Add error column and set to "ERROR" for rows causing issues
        df["error"] = "ERROR"
        df.to_sql(CsvData.__tablename__, engine, if_exists="append", index=False)
    finally:
        # Close the session
        session.close()


def parse_csv_file(file):
    # Read the file contents and decode them as UTF-8
    file_contents = file.read().decode("utf-8").splitlines()

    # Create an empty dictionary to store the column-value pairs
    data = {}

    # Use the csv module to read the file line by line
    reader = csv.reader(file_contents)
    for row in reader:
        if len(row) >= 3:
            column_name = row[0].strip()
            column_value = row[2].strip().replace(",", "").replace(" ", "")
            if column_name and column_value:
                if column_name in data:
                    column_name += "_2"  # Add suffix to duplicate column name
                data[column_name] = float(column_value)

    # Create a DataFrame from the dictionary
    print(f"TADYDATA {data}")
    df = pd.DataFrame([data])

    return df


while True:
    try:
        # Check for new CSV files in the watch directory
        for filename in sftp.listdir(watch_dir):
            if filename.endswith(".csv"):
                filepath = f"{watch_dir}/{filename}"

                # Load the CSV data into a DataFrame
                try:
                    with sftp.open(filepath, "r") as file:
                        df = parse_csv_file(file)
                        process_dataframe(df, filename)
                except Exception as e:
                    print(f"An error occurred while reading the CSV file: {e}")

                # Remove the CSV file after processing
                sftp.remove(filepath)

        # Sleep for an hour
        time.sleep(3600)

    except Exception as e:
        print(f"An error occurred: {e}")
