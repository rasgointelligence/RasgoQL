"""
RasgoQL package entrypoint
"""
from typing import Union

import webbrowser

from rasgoql.data import (
    DWCredentials,
    BigQueryCredentials, BigQueryDataWarehouse,
    SnowflakeCredentials, SnowflakeDataWarehouse
)
from rasgoql.errors import ParameterException
from rasgoql.main import RasgoQL
from rasgoql.version import __version__

__all__ = [
    '__version__',
    'docs',
    'connect',
]

DW_MAP = {
    'snowflake': SnowflakeDataWarehouse,
    'bigquery': BigQueryDataWarehouse,
    #'postgres': PostgresDataWarehouse,
}


def docs():
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
