"""
Base DataWarehouse classes
"""
from abc import ABC
from typing import Union

import pandas as pd


class DWCredentials(ABC):
    """
    Base DW Credentials
    """
    dw_type = None

    @classmethod
    def from_dict(
            cls,
            source_dict: dict
        ) -> 'DWCredentials':
        """
        Creates an instance of this Class from a dict
        """
        raise NotImplementedError()

    @classmethod
    def from_env(
            cls,
            file_path: str = None
        ) -> 'DWCredentials':
        """
        Creates an instance of this Class from a .env file on your machine
        """
        raise NotImplementedError()

    def to_dict(self) -> dict:
        """
        Returns a dict of the credentials
        """
        raise NotImplementedError()

    def to_env(
            self,
            file_path: str,
            overwrite: bool
        ):
        """
        Saves credentials to a .env file on your machine
        """
        raise NotImplementedError()

class DataWarehouse(ABC):
    """
    Base DW Class
    """
    dw_type = None
    credentials_class = DWCredentials

    def __init__(self):
        self.credentials = None
        self.connection = None

    def connect(
            self,
            credentials: Union[dict, DWCredentials]
        ):
        """
        Connect to this DataWarehouse
        """
        raise NotImplementedError()

    # ---------------------------
    # Core Data Warehouse methods
    # ---------------------------

    def close_connection(self):
        """
        Close connection to this DataWarehouse
        """
        raise NotImplementedError()

    def create(
            self,
            sql: str,
            fqtn: str,
            table_type: str = 'VIEW',
            overwrite: bool = False
        ):
        """
        Create a view or table from given SQL
        """
        raise NotImplementedError()

    def define_schema(
            self,
            fqtn: str
        ) -> dict:
        """
        Return the schema of a table or view
        """
        raise NotImplementedError()

    def execute_query(
            self,
            sql: str,
            response: str = 'tuple',
            acknowledge_risk: bool = False
        ):
        """
        Run a query against this DataWarehouse
        """
        raise NotImplementedError()

    def get_ddl(
            self,
            fqtn: str
        ) -> str:
        """
        Returns the create statement for a table or view
        """
        raise NotImplementedError()

    def get_object_details(
            self,
            fqtn: str
        ) -> tuple:
        """
        Return details of a table or view in this DataWarehouse
        """
        raise NotImplementedError()

    def list_tables(
            self,
            database: str = None,
            schema: str = None
        ):
        """
        List all available tables in this DataWarehouse
        """
        raise NotImplementedError()

    def preview(
            self,
            sql: str,
            limit: int = 10
        ) -> pd.DataFrame:
        """
        Returns 10 records into a pandas DataFrame
        """
        raise NotImplementedError()

    def save_df(
            self,
            df: pd.DataFrame,
            fqtn: str,
            method: str = None
        ):
        """
        Creates a table in this DataWarehouse from a pandas Dataframe
        """
        raise NotImplementedError()

    # ---------------------------
    # Core Data Warehouse helpers
    # ---------------------------
    def _table_exist(
            self,
            fqtn: str
        ) -> bool:
        """
        Check for existence of fqtn in this Data Warehouse and return a boolean
        """
        raise NotImplementedError()

    def _validate_namespace(
            self,
            namespace: str
        ):
        """
        Checks a namespace string for compliance with this DataWarehouse format
        """
        raise NotImplementedError()
