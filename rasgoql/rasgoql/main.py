"""
RasgoQL main class
"""
from typing import List, Type

import pandas as pd
import rasgotransforms as rtx

from rasgoql.data import DataWarehouse
from rasgoql.primitives import Dataset, SQLChain, TransformTemplate
from rasgoql.utils.messaging import set_verbose
from rasgoql.version import __version__

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
        return rtx.serve_rasgo_transform_templates(self._dw.dw_type)

    def dataset(
            self,
            fqtn: str
        ) -> Dataset:
        """
        Returns a Dataset connected to the Cloud Data Warehouse
        """
        return Dataset(fqtn, self._dw)

    def dataset_from_df(
            self,
            df: pd.DataFrame,
            table_name: str,
            method: str = None
        ) -> Dataset:
        """
        Writes a pandas Dataframe to a table in your Data Warehouse
        and returns it as a Dataset

        Params:
        `df`: pandas DataFrame:
            DataFrame to upload
        `table_name`: str:
            Name for the new table
        `method`: str
            Values: [append, replace]
            when this table already exists in your DataWarehouse,
            pass append: to add dataframe rows to it
            pass replace: to overwrite it with dataframe rows
                WARNING: This will completely overwrite data in the existing table
        """
        fqtn = self._dw.save_df(df, table_name, method)
        return Dataset(fqtn, self._dw)

    def define_transform(
            self,
            name: str
        ) -> str:
        """
        Returns full details of a RasgoQL transform
        """
        udt: TransformTemplate = None
        for t in rtx.serve_rasgo_transform_templates(self._dw.dw_type):
            if t.name == name:
                udt = t
        if udt:
            return udt.define()
        raise ValueError(f'{name} is not a valid Tranform name. ' \
                           'Run `.list_transforms()` to see available transforms.')

    def query(
            self,
            sql: str,
            acknowledge_risk: bool = False
        ):
        """
        Execute a query against your Data Warehouse
        """
        return self._dw.execute_query(sql, acknowledge_risk=acknowledge_risk)

    def query_into_df(
            self,
            sql: str,
            acknowledge_risk: bool = False
        ) -> pd.DataFrame:
        """
        Execute a query against your Data Warehouse and return results in a pandas DataFrame
        """
        return self._dw.execute_query(sql, response='df', acknowledge_risk=acknowledge_risk)

    def set_verbose(
            self,
            value: bool
        ) -> None:
        """
        Turn verbose logging on or off

        value: bool
            True = log more info about SQL and primitive activities
            False = log almost nothing
        """
        set_verbose(value)

    def sqlchain(
            self,
            fqtn: str,
            namespace: str = None
        ) -> SQLChain:
        """
        Returns a SQLChain connected to the Cloud Data Warehouse
        """
        return SQLChain(
            entry_table=Dataset(
                fqtn,
                self._dw
            ),
            namespace=namespace or self._dw.default_namespace,
            dw=self._dw
        )
