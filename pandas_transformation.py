# pandas_transformation.py


def simple_transform(df):
    """
    This function applies a simple transformation to the DataFrame,
    in this case, creating a new column that is the sum of all other columns.
    Modify this function based on your actual data transformation needs.
    """
    df["sum_column"] = df.sum(axis=1)
    return df
