"""
RasgoQL main class
"""
from typing import List, Type

import pandas as pd
import rasgotransforms as rtx

from .data import DataWarehouse
from .primitives import Dataset, SQLChain, TransformTemplate
from .version import __version__

class RasgoQL:
    """
    Entry class for rasgoql
    """
    __version__ = __version__

    def __init__(
            self,
            connection: Type[DataWarehouse],
            credentials: dict
        ):
        self.credentials = credentials
        self._dw = connection()
        self._dw.connect(credentials)

    def disconnect_dw(self):
        """
        Closes a connection to the Data Warehouse
        """
        self._dw.close_connection()

    def list_tables(
            self,
            database: str = None,
            schema: str = None
        ):
        """
        Returns a list of tables in your Data Warehouse
        """
        return self._dw.list_tables(database, schema)

    def list_transforms(self) -> List[TransformTemplate]:
        """
        Returns a list of RasgoQL transforms
        """
        return rtx.serve_rasgo_transform_templates()

    def dataset(
            self,
            fqtn: str
        ) -> Dataset:
        """
        Returns a Dataset connected to the Cloud Data Warehouse
        """
        return Dataset(fqtn, self._dw)

    def define_transform(self, name: str) -> str:
        """
        Returns full details of a RasgoQL transform
        """
        udt: TransformTemplate = None
        for t in rtx.serve_rasgo_transform_templates():
            if t.name == name:
                udt = t
        if udt:
            return udt.define()
        raise ValueError(f'{name} is not a valid Tranform name. ' \
                           'Run `.list_transforms()` to see available transforms.')

    def query(
            self,
            sql: str
        ):
        """
        Execute a query against your Data Warehouse
        """
        return self._dw.execute_query(sql)

    def query_into_df(
            self,
            sql: str
        ) -> pd.DataFrame:
        """
        Execute a query against your Data Warehouse and return results in a pandas DataFrame
        """
        return self._dw.execute_query(sql, response='df')

    def sqlchain(
            self,
            fqtn: str
        ) -> SQLChain:
        """
        Returns a SQLChain connected to the Cloud Data Warehouse
        """
        return SQLChain(
            entry_table=Dataset(
                fqtn,
                self._dw
            ),
            dw=self._dw
        )
