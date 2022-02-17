"""
Enums
"""
from enum import Enum

from rasgoql.errors import ParameterException


def _wrap_in_quotes(string: str) -> str:
    return f"'{string}'"

class DWType(Enum):
    """
    Supported Data Warehouses
    """
    BIGQUERY = 'bigquery'
    #FUTURE: POSTGRES = 'postgres'
    SNOWFLAKE = 'snowflake'

def check_data_warehouse(input_value: str):
    """
    Warn if an incorrect Data Warehouse is passed
    """
    supported_dws = _wrap_in_quotes("', '".join([e.value for e in DWType]))
    try:
        DWType[input_value.upper()]
    except Exception:
        raise ParameterException(f'data_warehouse parameter accepts values: {supported_dws}')
    return input_value.upper()


class TableState(Enum):
    """
    State of a table
    """
    IN_DW = 'in dw'
    IN_MEMORY = 'in memory'
    UNKNOWN = 'unknown'

def check_table_state(input_value: str):
    """
    Warn if an incorrect table state is passed
    """
    table_states = _wrap_in_quotes("', '".join([e.value for e in TableState]))
    try:
        TableState[input_value.upper()]
    except Exception:
        raise ParameterException(f'table_state parameter accepts values: {table_states}')
    return input_value.upper()


class TableType(Enum):
    """
    Type of table in a DW
    """
    TABLE = 'table'
    VIEW = 'view'
    UNKNOWN = 'unknown'

def check_table_type(input_value: str):
    """
    Warn if an incorrect table type is passed
    """
    table_types = _wrap_in_quotes("', '".join([e.value for e in TableType]))
    try:
        TableType[input_value.upper()]
    except Exception:
        raise ParameterException(f'table_type parameter accepts values: {table_types}')
    return input_value.upper()


class RenderMethod(Enum):
    """
    Ways to render a sql chain
    """
    SELECT = 'select'
    TABLE = 'table'
    VIEW = 'view'
    VIEWS = 'views'

def check_render_method(input_value: str):
    """
    Warn if an incorrect render method is passed
    """
    render_methods = _wrap_in_quotes("', '".join([e.value for e in RenderMethod]))
    try:
        RenderMethod[input_value.upper()]
    except Exception:
        raise ParameterException(f'render_method parameter accepts values: {render_methods}')
    return input_value.upper()


class ResponseType(Enum):
    """
    Formats to return query results
    """
    DICT = 'dict'
    DF = 'df'
    TUPLE = 'tuple'
    NONE = 'none'

def check_response_type(input_value: str):
    """
    Warn if an incorrect response type is passed
    """
    response_types = _wrap_in_quotes("', '".join([e.value for e in ResponseType]))
    try:
        ResponseType[input_value.upper()]
    except Exception:
        raise ParameterException(f'response parameter accepts values: {response_types}')
    return input_value.upper()


class WriteMethod(Enum):
    """
    Ways to write data
    """
    APPEND = 'append'
    REPLACE = 'replace'
    UPSERT = 'upsert'

def check_write_method(input_value: str):
    """
    Warn if an incorrect write method is passed
    """
    write_methods = _wrap_in_quotes("', '".join([e.value for e in WriteMethod]))
    try:
        WriteMethod[input_value.upper()]
    except Exception:
        raise ParameterException(f'method parameter accepts values: {write_methods}')
    return input_value.upper()
