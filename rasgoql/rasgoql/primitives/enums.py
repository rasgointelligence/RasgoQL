"""
Enums
"""
from enum import Enum
import logging

from rasgoql.errors import ParameterException

logging.basicConfig()
logger = logging.getLogger('Enums')
logger.setLevel(logging.INFO)


def _wrap_in_quotes(string: str) -> str:
    return f"'{string}'"

class DWType(Enum):
    """
    Supported Data Warehouses
    """
    BIGQUERY = 'BIGQUERY'
    #FUTURE: POSTGRES = 'POSTGRES'
    SNOWFLAKE = 'SNOWFLAKE'

def check_data_warehouse(input_value: str):
    """
    Warn if an incorrect Data Warehouse is passed
    """
    supported_dws = _wrap_in_quotes("', '".join([e.value for e in DWType]))
    try:
        output_value = DWType[input_value.upper()].value
    except Exception:
        raise ParameterException(f'data_warehouse parameter accepts values: {supported_dws}')
    return output_value


class TableState(Enum):
    """
    State of a table
    """
    IN_DW = 'IN DW'
    IN_MEMORY = 'IN MEMORY'
    UNKNOWN = 'UNKNOWN'

def check_table_state(input_value: str):
    """
    Warn if an incorrect table state is passed
    """
    table_states = _wrap_in_quotes("', '".join([e.value for e in TableState]))
    try:
        output_value = TableState[input_value.upper()].value
    except Exception:
        raise ParameterException(f'table_state parameter accepts values: {table_states}')
    return output_value


class TableType(Enum):
    """
    Type of table in a DW
    """
    EXTERNAL = 'EXTERNAL'
    TABLE = 'TABLE'
    TEMPORARY = 'TEMPORARY'
    UNKNOWN = 'UNKNOWN'
    VIEW = 'VIEW'

def check_table_type(input_value: str):
    """
    Warn if an incorrect table type is passed
    """
    table_types = _wrap_in_quotes("', '".join([e.value for e in TableType]))
    try:
        output_value = TableType[input_value.upper()].value
    except Exception:
        logger.warning(
            f'{input_value} is an unexpected value for table_type. '
            f'Expected values are: {table_types}. '
            'Defaulting to UNKNOWN type.')
        return TableType.UNKNOWN.value
    return output_value


class RenderMethod(Enum):
    """
    Ways to render a sql chain
    """
    SELECT = 'SELECT'
    TABLE = 'TABLE'
    VIEW = 'VIEW'
    VIEWS = 'VIEWS'

def check_render_method(input_value: str):
    """
    Warn if an incorrect render method is passed
    """
    render_methods = _wrap_in_quotes("', '".join([e.value for e in RenderMethod]))
    try:
        output_value = RenderMethod[input_value.upper()].value
    except Exception:
        raise ParameterException(f'render_method parameter accepts values: {render_methods}')
    return output_value


class ResponseType(Enum):
    """
    Formats to return query results
    """
    DICT = 'DICT'
    DF = 'DF'
    TUPLE = 'TUPLE'
    NONE = 'NONE'

def check_response_type(input_value: str):
    """
    Warn if an incorrect response type is passed
    """
    response_types = _wrap_in_quotes("', '".join([e.value for e in ResponseType]))
    try:
        output_value = ResponseType[input_value.upper()].value
    except Exception:
        raise ParameterException(f'response parameter accepts values: {response_types}')
    return output_value


class WriteMethod(Enum):
    """
    Ways to write data
    """
    APPEND = 'APPEND'
    REPLACE = 'REPLACE'
    UPSERT = 'UPSERT'

def check_write_method(input_value: str):
    """
    Warn if an incorrect write method is passed
    """
    write_methods = _wrap_in_quotes("', '".join([e.value for e in WriteMethod]))
    try:
        output_value = WriteMethod[input_value.upper()].value
    except Exception:
        raise ParameterException(f'method parameter accepts values: {write_methods}')
    return output_value


class WriteTableType(Enum):
    """
    Type of table in a DW
    """
    TABLE = 'TABLE'
    VIEW = 'VIEW'

def check_write_table_type(input_value: str):
    """
    Warn if an incorrect table type is passed
    """
    table_types = _wrap_in_quotes("', '".join([e.value for e in WriteTableType]))
    try:
        output_value = WriteTableType[input_value.upper()].value
    except Exception:
        raise ParameterException(f'table_type parameter accepts values: {table_types}')
    return output_value
