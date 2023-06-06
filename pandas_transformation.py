import re
from collections import defaultdict
from models import CsvData
import pandas as pd


def simple_transform(df):
    # Make column names lower case
    df.columns = df.columns.str.lower()

    # Replace spaces with underscores
    df.columns = df.columns.str.replace(" ", "_")

    # Replace brackets and their contents with nothing
    df.columns = df.columns.str.replace(r"\(.*\)", "")

    # Replace dots with underscores
    df.columns = df.columns.str.replace(".", "_")

    # Replace multiple consecutive underscores with a single one
    df.columns = df.columns.str.replace(r"_+", "_")

    # Replace any remaining non-alphanumeric characters (except underscores) with nothing
    df.columns = df.columns.map(
        lambda x: re.sub(r"[^\w\s]", "", x) if x is not None else None
    )

    # Handle duplicate column names
    suffixes = defaultdict(int)
    new_columns = []
    for col in df.columns:
        if col in new_columns or col == "":
            suffixes[col] += 1
            new_columns.append(f"{col}_{suffixes[col]}")
        else:
            new_columns.append(col)

    df.columns = new_columns

    # Filter out columns with empty headers
    df = df.loc[:, ~df.columns.isna()]

    # Print header names
    print(
        "Header names:", [col if col is not None else "" for col in df.columns.tolist()]
    )

    return df


def remove_suffix_columns(df):
    suffixes = ["_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9"]
    columns_to_drop = [col for col in df.columns if col.endswith(tuple(suffixes))]
    df = df.drop(columns_to_drop, axis=1)
    return df


def validate_dataframe(df):
    # Getting all columns from the CsvData model
    model_fields = CsvData.__table__.columns.keys()
    # Removing 'id' from model_fields as it's not part of the CSV data
    model_fields.remove("id")

    df_columns = df.columns.tolist()

    # Check if all columns in the DataFrame are in the model
    if all(elem in model_fields for elem in df_columns):
        return df
    else:
        # Identify columns present in the DataFrame but not in the model
        missing_columns = [col for col in df_columns if col not in model_fields]
        print(
            f"The following DataFrame columns are not in the model: {missing_columns}"
        )
        return df
