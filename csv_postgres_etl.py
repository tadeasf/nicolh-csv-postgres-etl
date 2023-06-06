# etl_script.py

import time
import json
import pandas as pd
from paramiko import Transport, SFTPClient
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

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
    SFTP_PORT,
)

# Connect to the PostgreSQL database
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Set up the SFTP connection
transport = Transport((SFTP_HOST, SFTP_PORT))
transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

sftp = SFTPClient.from_transport(transport)

# The directory to watch for new CSV files
watch_dir = "home/tadeas/nicolh/csvs"


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
    if not engine.dialect.has_table(engine, CsvData.__tablename__):
        Base.metadata.create_all(engine)

    # Remove duplicates
    query = text("""SELECT * FROM csv_data""")
    existing_data = pd.read_sql_query(query, con=engine)
    df = pd.concat([df, existing_data]).drop_duplicates(keep=False)

    # Append data from the DataFrame to the table
    try:
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


while True:
    try:
        # Check for new CSV files in the watch directory
        for filename in sftp.listdir(watch_dir):
            if filename.endswith(".csv"):
                filepath = f"{watch_dir}/{filename}"

                # Load the CSV data into a DataFrame in chunks for large files
                chunksize = (
                    50000  # adjust this value depending on your available memory
                )
                for chunk in pd.read_csv(sftp.open(filepath, "r"), chunksize=chunksize):
                    process_dataframe(chunk, filename)

                # Remove the CSV file after processing
                sftp.remove(filepath)

        # Sleep for an hour
        time.sleep(3600)

    except Exception as e:
        print(f"An error occurred: {e}")
