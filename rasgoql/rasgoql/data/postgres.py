"""
Postgres DataWarehouse classes
"""
import logging
import os
from typing import Union
from urllib.parse import quote_plus as urlquote

import json
import pandas as pd

from rasgoql.data.base import DWCredentials
from rasgoql.data.sqlalchemy import SQLAlchemyDataWarehouse

from rasgoql.errors import (
    DWCredentialsWarning,
    PackageDependencyWarning,
    TableConflictException,
)
from rasgoql.imports import alchemy_engine, alchemy_session
from rasgoql.primitives.enums import check_table_type
from rasgoql.utils.creds import load_env, save_env
from rasgoql.utils.messaging import verbose_message

logging.basicConfig()
logger = logging.getLogger("Postgres DataWarehouse")
logger.setLevel(logging.INFO)


class PostgresCredentials(DWCredentials):
    """
    Postgres Credentials
    """

    dw_type = "postgresql"

    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: str,
        database: str,
        schema: str,
    ):
        if alchemy_engine is None:
            raise PackageDependencyWarning(
                "Missing a required python package to run Postgres. "
                "Please download the Postgres package by running: "
                "pip install rasgoql[postgres]"
            )
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.schema = schema

    def __repr__(self) -> str:
        return json.dumps(
            {
                "user": self.username,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "schema": self.schema,
            }
        )

    @classmethod
    def from_env(cls, filepath: str = None) -> "PostgresCredentials":
        """
        Creates an instance of this Class from a .env file on your machine
        """
        load_env(filepath)
        username = os.getenv("POSTGRES_USERNAME")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("POSTGRES_DATABASE")
        schema = os.getenv("POSTGRES_SCHEMA")
        if not all([username, password, host, port, database, schema]):
            raise DWCredentialsWarning(
                "Your env file is missing expected credentials. Consider running "
                "PostgresCredentials(*args).to_env() to repair this."
            )
        return cls(username, password, host, port, database, schema)

    def to_dict(self) -> dict:
        """
        Returns a dict of the credentials
        """
        return {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schema": self.schema,
            "dw_type": self.dw_type,
        }

    def to_env(self, filepath: str = None, overwrite: bool = False):
        """
        Saves credentials to a .env file on your machine
        """
        creds = {
            "POSTGRES_USERNAME": self.username,
            "POSTGRES_PASSWORD": self.password,
            "POSTGRES_HOST": self.host,
            "POSTGRES_PORT": self.port,
            "POSTGRES_DATABASE": self.database,
            "POSTGRES_SCHEMA": self.schema,
        }
        return save_env(creds, filepath, overwrite)


class PostgresDataWarehouse(SQLAlchemyDataWarehouse):
    """
    Postgres DataWarehouse
    """

    dw_type = "postgresql"
    credentials_class = PostgresCredentials

    def __init__(self):
        super().__init__()

    # ---------------------------
    # FQTN and namespace methods
    # ---------------------------
    def parse_table_and_schema_from_fqtn(
        self,
        fqtn: str
    ) -> tuple:
        """
        Accepts a possible FQTN and returns the schema and table from it
        """
        fqtn = self.validate_fqtn(fqtn)
        return tuple(fqtn.split(".")[1:])

    # ---------------------------
    # Core Data Warehouse methods
    # ---------------------------
    def change_namespace(self, namespace: str) -> None:
        """
        Changes the default namespace of your connection
        Params:
        `namespace`: str:
            namespace (database.schema)
        """
        raise NotImplementedError(
            "Connecting to a new Database in a single session is not supported by Postgres. "
            "Please build a new connection using the PostgresCredentials class"
        )

    def connect(self, credentials: Union[dict, PostgresCredentials]):
        """
        Connect to Postgres
        Params:
        `credentials`: dict:
            dict (or DWCredentials class) holding the connection credentials
        """
        if isinstance(credentials, PostgresCredentials):
            credentials = credentials.to_dict()

        try:
            self.credentials = credentials
            self.database = credentials.get("database")
            self.schema = credentials.get("schema")
            self.connection = alchemy_session(self._engine)
            verbose_message("Connected to Postgres", logger)
        except Exception as e:
            self._error_handler(e)

    def create(
        self, sql: str, fqtn: str, table_type: str = "VIEW", overwrite: bool = False
    ):
        """
        Create a view or table from given SQL
        Params:
        `sql`: str:
            query that returns data (i.e. can be wrapped in a CREATE TABLE statement)
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)
            Name for the new table
        `table_type`: str:
            One of values: [view, table]
        `overwrite`: bool
            pass True when this table name already exists in your DataWarehouse
            and you know you want to overwrite it
            WARNING: This will completely overwrite data in the existing table
        """
        table_type = check_table_type(table_type)
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        schema, table = self.parse_table_and_schema_from_fqtn(fqtn=fqtn)
        if self._table_exists(fqtn=fqtn) and not overwrite:
            msg = (
                f"A table or view named {fqtn} already exists. "
                "If you are sure you want to overwrite it, "
                "pass in overwrite=True and run this function again"
            )
            raise TableConflictException(msg)
        query = f"CREATE OR REPLACE {table_type} {schema}.{table} AS {sql}"
        self.execute_query(query, acknowledge_risk=True, response="None")
        return fqtn

    def get_ddl(self, fqtn: str) -> pd.DataFrame:
        """
        Returns a DataFrame describing the column in the table
        `fqtn`: str:
            Fully-qualified Table Name (database.schema.table)
        """
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        _, schema_name, table_name = self.parse_fqtn(fqtn)
        sql = (
            f"select table_schema, table_name, column_name, data_type, "
            f"character_maximum_length, column_default, is_nullable from "
            f"INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}' "
            f"and table_schema = '{schema_name}';"
        )
        query_response = self.execute_query(sql, response="DF")
        return query_response

    def get_object_details(self, fqtn: str) -> tuple:
        """
        Return details of a table or view in Postgres
        Params:
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)
        Response:
            object exists: bool
            is rasgo object: bool
            object type: [table|view|unknown]
        """
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        database, schema, table = self.parse_fqtn(fqtn)
        sql = (
            f"SELECT EXISTS(SELECT FROM pg_catalog.pg_class c JOIN "
            f"pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE "
            f"n.nspname = '{schema}' AND    c.relname = '{table}')"
        )
        result = self.execute_query(sql, response="dict")
        obj_exists = len(result) > 0
        is_rasgo_obj = False
        obj_type = "unknown"
        return obj_exists, is_rasgo_obj, obj_type

    # --------------------------
    # Postgres specific helpers
    # --------------------------
    @property
    def _engine(self) -> "alchemy_engine":
        """
        Returns a SQLAlchemy engine
        """
        engine_url = (
            f"{self.credentials.get('dw_type')}://"
            f"{self.credentials.get('username')}:"
            f"{urlquote(self.credentials.get('password'))}"
            f"@{self.credentials.get('host')}:"
            f"{self.credentials.get('port')}/"
            f"{self.credentials.get('database')}"
        )
        return alchemy_engine(engine_url)
