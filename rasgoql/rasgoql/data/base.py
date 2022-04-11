"""
Base DataWarehouse classes
"""
from __future__ import annotations
from abc import ABC
import re
import os
from typing import Union, Optional
from collections import namedtuple

import pandas as pd

FQTN = namedtuple("FQTN", ["database", "schema", "table"], defaults=(None, None, None))


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
            filepath: str = None
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
            filepath: str,
            overwrite: bool
        ):
        """
        Saves credentials to a .env file on your machine
        """
        raise NotImplementedError()

    @staticmethod
    def _parse_env_vars(prefix: str) -> dict[str, Union[str, bool, int]]:
        """
        Pull out all environment variables starting with a prefix
        ( usual same name as data warehouse, i.e. REDSHIFT )
        Auto-detect and convert integers and boolean values.
        """
        prefix = prefix.upper()
        if not prefix.endswith("_"):
            prefix = f"{prefix}_"

        env_vars = {}
        for var_name, value in os.environ.items():
            if var_name.upper().startswith(prefix):
                # Convert Booleans
                if value.lower().strip() in ("false", "true"):
                    value = False if value.lower().strip() == "false" else True
                # Convert Integers
                elif value.strip().isnumeric():
                    value = int(value.strip())
                env_vars[var_name.lower()[len(prefix):]] = value
        return env_vars


class DataWarehouse(ABC):
    """
    Base DW Class
    """
    dw_type = None
    credentials_class = DWCredentials

    def __init__(self):
        self.credentials = None
        self.connection = None

    # ---------------------------
    # FQTN and namespace methods
    # ---------------------------
    def magic_fqtn_handler(
        self,
        possible_fqtn: str,
        default_namespace: str
    ) -> str:
        """
        Makes all of your wildest dreams come true... well not *that* one
        """
        input_db, input_schema, table = self.parse_fqtn(possible_fqtn, default_namespace, False)
        default_database, default_schema = self.parse_namespace(default_namespace)
        database = input_db or default_database
        schema = input_schema or default_schema
        return self.make_fqtn(database, schema, table)

    def make_fqtn(
        self,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Accepts component parts and returns a fully qualified table string
        """
        return f"{database}.{schema}.{table}"

    def make_namespace_from_fqtn(
        self,
        fqtn: str
    ) -> str:
        """
        Accepts component parts and returns a fully qualified namespace string
        """
        database, schema, _ = self.parse_fqtn(fqtn)
        return f"{database}.{schema}"

    def parse_fqtn(
        self,
        fqtn: str,
        default_namespace: Optional[str] = None,
        strict: bool = True
    ) -> FQTN:
        """
        Accepts a possible fully qualified table string and returns its component parts
        """
        if strict:
            fqtn = self.validate_fqtn(fqtn)
            return FQTN(* fqtn.split("."))

        if fqtn.count(".") == 2:
            return FQTN(* fqtn.split("."))

        if not default_namespace:
            raise ValueError(f'{fqtn} is not a well-formed fqtn')

        database, schema = self.parse_namespace(default_namespace)

        if fqtn.count(".") == 1:
            return FQTN(database, * fqtn.split("."))
        if fqtn.count(".") == 0:
            return FQTN(database, schema, fqtn)
        raise ValueError(f'{fqtn} is not a well-formed fqtn')

    def parse_namespace(
        self,
        namespace: str
    ) -> tuple:
        """
        Accepts a possible namespace string and returns its component parts
        """
        namespace = self.validate_namespace(namespace)
        return tuple(namespace.split("."))

    def validate_fqtn(self, fqtn: str) -> str:
        """
        Accepts a possible fully qualified table string and decides whether it is well-formed
        """
        if re.match(r'^[^\s]+\.[^\s]+\.[^\s]+', fqtn):
            return fqtn
        raise ValueError(f'{fqtn} is not a well-formed fqtn')

    def validate_namespace(
        self,
        namespace: str
    ) -> str:
        """
        Accepts a possible namespace string and decides whether it is well-formed
        """
        if re.match(r'^[^\s]+\.[^\s]+', namespace):
            return namespace
        raise ValueError(f'{namespace} is not a well-formed namespace')

    # Core methods

    def change_namespace(
            self,
            namespace: str
    ):
        """
        Change the default namespace of this connection
        """
        raise NotImplementedError()

    def connect(
            self,
            credentials: Union[dict, DWCredentials]
        ):
        """
        Connect to this DataWarehouse
        """
        raise NotImplementedError()

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

    @property
    def default_namespace(self) -> str:
        """
        Returns the default namespace of this connection
        """
        raise NotImplementedError()

    @default_namespace.setter
    def default_namespace(
        self,
        new_namespace: str
    ):
        """
        Setter method for the `default_namespace` property
        """
        raise NotImplementedError()

    def execute_query(
            self,
            sql: str,
            response: str = 'tuple',
            acknowledge_risk: bool = False,
            **kwargs
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

    def get_schema(
            self,
            fqtn: str,
            create_sql: str = None
        ) -> dict:
        """
        Return the schema of a table or view
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
    def _table_exists(
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
