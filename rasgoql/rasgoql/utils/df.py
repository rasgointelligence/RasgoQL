"""
Helpful DataFrame utilities
"""
import pandas as pd
from pandas.io.json import build_table_schema

from .sql import cleanse_sql_name


def build_dataframe_schema(
        df: pd.DataFrame,
        include_index=False
    ) -> dict:
    """
    Returns a dict representing the schema of a dataframe
    """
    schema_list = build_table_schema(df)
    if not include_index:
        return {column['name']: column
                for column in schema_list['fields'] if column['name'] != 'index'}
    return {column['name']: column
            for column in schema_list['fields']}

def cleanse_sql_dataframe(
        df: pd.DataFrame
    ):
    """
    Renames all columns in a pandas dataframe to SQL compliant names in place
    """
    df.rename(columns={r: cleanse_sql_name(r) for r in build_dataframe_schema(df)},
                inplace=True)

def generate_dataframe_ddl(
        df: pd.DataFrame,
        table_name: str
    ) -> str:
    """
    Generates a SQL statement to create a table matching the schema of a dataframe
    """
    create_statement = "CREATE OR REPLACE TABLE"
    return pd.io.sql.get_schema(df, table_name) \
        .replace("CREATE TABLE", create_statement) \
        .replace('"', '')

def map_pandas_data_type(
        data_type: str
    ) -> str:
    """
    For a given pandas data type, return the equivalent python type
    """
    if data_type.startswith('float'):
        return 'float'
    if data_type.startswith('datetime'):
        return 'datetime'
    if data_type.startswith('int'):
        return 'integer'
    return data_type

def tag_dataframe(
        df: pd.DataFrame,
        attribute: dict
    ):
    """
    Tags a dataframe with a user-defined attribute
    """
    write_back_attrs = df.attrs or {}
    write_back_attrs.update(attribute)
    df.attrs = write_back_attrs
