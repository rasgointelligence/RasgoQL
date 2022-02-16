"""
Enums
"""
from enum import Enum

from rasgoql.errors import ParameterException


def _wrap_in_quotes(string: str) -> str:
    return "'"+string+"'"

class DWType(Enum):
    """
    Supported Data Warehouses
    """
    #FUTURE: BIGQUERY = 'bigquery'
    #FUTURE: POSTGRES = 'postgres'
    SNOWFLAKE = 'snowflake'

SUPPORTED_DWS = _wrap_in_quotes("', '".join([e.value for e in DWType]))

def check_data_warehouse(input_value: str):
    """
    Warn if an incorrect Data Warehouse is passed
    """
    try:
        DWType[input_value.upper()]
    except Exception:
        raise ParameterException(f'data_warehouse parameter accepts values: {SUPPORTED_DWS}')
    return input_value.upper()


class TableState(Enum):
    """
    State of a table
    """
    IN_DW = 'in dw'
    IN_MEMORY = 'in memory'
    UNKNOWN = 'unknown'

TABLE_STATES = _wrap_in_quotes("', '".join([e.value for e in TableState]))

def check_table_state(input_value: str):
    """
    Warn if an incorrect table state is passed
    """
    try:
        TableState[input_value.upper()]
    except Exception:
        raise ParameterException(f'table_state parameter accepts values: {TABLE_STATES}')
    return input_value.upper()


class TableType(Enum):
    """
    Type of table in a DW
    """
    TABLE = 'table'
    VIEW = 'view'
    UNKNOWN = 'unknown'

TABLE_TYPES = _wrap_in_quotes("', '".join([e.value for e in TableType]))

def check_table_type(input_value: str):
    """
    Warn if an incorrect table type is passed
    """
    try:
        TableType[input_value.upper()]
    except Exception:
        raise ParameterException(f'table_type parameter accepts values: {TABLE_TYPES}')
    return input_value.upper()


class RenderMethod(Enum):
    """
    Ways to render a sql chain
    """
    SELECT = 'select'
    TABLE = 'table'
    VIEW = 'view'
    VIEWS = 'views'

RENDER_METHODS = _wrap_in_quotes("', '".join([e.value for e in RenderMethod]))

def check_render_method(input_value: str):
    """
    Warn if an incorrect render method is passed
    """
    try:
        RenderMethod[input_value.upper()]
    except Exception:
        raise ParameterException(f'render_method parameter accepts values: {RENDER_METHODS}')
    return input_value.upper()


class ResponseType(Enum):
    """
    Formats to return query results
    """
    DICT = 'dict'
    DF = 'df'
    TUPLE = 'tuple'
    NONE = 'none'

RESPONSE_TYPES = _wrap_in_quotes("', '".join([e.value for e in ResponseType]))

def check_response_type(input_value: str):
    """
    Warn if an incorrect response type is passed
    """
    try:
        ResponseType[input_value.upper()]
    except Exception:
        raise ParameterException(f'response parameter accepts values: {RESPONSE_TYPES}')
    return input_value.upper()


class WriteMethod(Enum):
    """
    Ways to write data
    """
    APPEND = 'append'
    REPLACE = 'replace'
    UPSERT = 'upsert'

WRITE_METHODS = _wrap_in_quotes("', '".join([e.value for e in WriteMethod]))

def check_write_method(input_value: str):
    """
    Warn if an incorrect write method is passed
    """
    try:
        WriteMethod[input_value.upper()]
    except Exception:
        raise ParameterException(f'method parameter accepts values: {WRITE_METHODS}')
    return input_value.upper()
