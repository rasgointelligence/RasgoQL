"""
RasgoQL package entrypoint
"""
from typing import Union

import webbrowser

from .data import DWCredentials, SnowflakeCredentials, SnowflakeDataWarehouse
from .errors import ParameterException
from .main import RasgoQL
from .version import __version__

__all__ = [
    '__version__',
    'open_docs',
    'connect',
]

DW_MAP = {
    'snowflake': SnowflakeDataWarehouse,
    #'bigquery': BigQueryDataWarehouse,
    #'postgres': PostgresDataWarehouse,
}


def open_docs():
    """
    Open Rasgo Docs page
    """
    url = 'https://docs.rasgoql.com'
    webbrowser.open(url)
    print(url)

def connect(
        credentials: DWCredentials
    ):
    """
    Return a RasgoQL object connected to a Data Warehouse
    """
    return RasgoQL(
        connection=DW_MAP[credentials.dw_type],
        credentials=credentials.to_dict()
    )
