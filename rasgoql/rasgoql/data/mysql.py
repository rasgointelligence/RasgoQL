"""
MySQL DataWarehouse classes
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
    ParameterException,
)
from rasgoql.imports import alchemy_engine, alchemy_session
from rasgoql.primitives.enums import check_table_type
from rasgoql.utils.creds import load_env, save_env
from rasgoql.utils.messaging import verbose_message
from rasgoql.utils.sql import magic_fqtn_handler, parse_fqtn, validate_namespace

logging.basicConfig()
logger = logging.getLogger("MySQL DataWarehouse")
logger.setLevel(logging.INFO)


class MySQLCredentials(DWCredentials):
    """
    MySQL Credentials
    """

    dw_type = "mysql"
    db_api = "pymysql"

    def __init__(self, username: str, password: str, host: str, database: str):
        if alchemy_engine is None:
            raise PackageDependencyWarning(
                "Missing a required python package to run MySQL. "
                "Please download the MySQL package by running: "
                "pip install rasgoql[mysql]"
            )
        self.username = username
        self.password = password
        self.host = host
        self.database = database

    def __repr__(self) -> str:
        return json.dumps(
            {
                "user": self.username,
                "host": self.host,
                "database": self.database,
            }
        )

    @classmethod
    def from_env(cls, filepath: str = None) -> "MySQLCredentials":
        """
        Creates an instance of this Class from a .env file on your machine
        """
        load_env(filepath)
        username = os.getenv("MYSQL_USERNAME")
        password = os.getenv("MYSQL_PASSWORD")
        host = os.getenv("MYSQL_HOST")
        database = os.getenv("MYSQL_DATABASE")
        if not all([username, password, host, database]):
            raise DWCredentialsWarning(
                "Your env file is missing expected credentials. Consider running "
                "MySQLCredentials(*args).to_env() to repair this."
            )
        return cls(username, password, host, database)

    def to_dict(self) -> dict:
        """
        Returns a dict of the credentials
        """
        return {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "database": self.database,
            "dw_type": self.dw_type,
            "db_api": self.db_api,
        }

    def to_env(self, filepath: str = None, overwrite: bool = False):
        """
        Saves credentials to a .env file on your machine
        """
        creds = {
            "MYSQL_USERNAME": self.username,
            "MYSQL_PASSWORD": self.password,
            "MYSQL_HOST": self.host,
            "MYSQL_DATABASE": self.database,
        }
        return save_env(creds, filepath, overwrite)


class MySQLDataWarehouse(SQLAlchemyDataWarehouse):
    """
    MySQL DataWarehouse
    """

    dw_type = "mysql"
    credentials_class = MySQLCredentials

    def __init__(self):
        super().__init__()

    # ---------------------------
    # Core Data Warehouse methods
    # ---------------------------

    @property
    def default_namespace(self) -> str:
        """
        Returns the default database of this connection
        """
        return f"{self.database}"

    @default_namespace.setter
    def default_namespace(self, new_namespace: str):
        self.database = new_namespace

    def _validate_namespace(self, namespace: str) -> str:
        """
        Checks a namespace string for compliance with required format

        Params:
        `namespace`: str:
            namespace (database)
        """
        if "." in namespace:
            raise ParameterException("MySQL Namespaces should be format: DATABASE")
        return namespace.upper()

    def change_namespace(self, namespace: str) -> None:
        """
        Changes the default namespace of your connection

        Params:
        `namespace`: str:
            namespace (database.schema)
        """
        raise NotImplementedError(
            "Connecting to a new Database in a single session is not supported by MySQL. "
            "Please build a new connection using the MySQLCredentials class"
        )

    def connect(self, credentials: Union[dict, MySQLCredentials]):
        """
        Connect to Postgres

        Params:
        `credentials`: dict:
            dict (or DWCredentials class) holding the connection credentials
        """
        if isinstance(credentials, MySQLCredentials):
            credentials = credentials.to_dict()

        try:
            self.credentials = credentials
            self.database = credentials.get("database")
            self.schema = credentials.get("schema")
            self.connection = alchemy_session(self._engine)
            verbose_message("Connected to MySQL", logger)
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
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        if self._table_exists(fqtn=fqtn) and not overwrite:
            msg = (
                f"A table or view named {fqtn} already exists. "
                "If you are sure you want to overwrite it, "
                "pass in overwrite=True and run this function again"
            )
            raise TableConflictException(msg)
        query = f"CREATE OR REPLACE {table_type} {fqtn} AS {sql}"
        self.execute_query(query, acknowledge_risk=True, response="None")
        return fqtn

    def get_ddl(self, fqtn: str) -> pd.DataFrame:
        """
        Returns a DataFrame describing the column in the table

        `fqtn`: str:
            Fully-qualified Table Name (database.schema.table)
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        db, _, table_name = parse_fqtn(
            fqtn, default_namespace=self.default_namespace, strict=False
        )
        sql = f"SHOW CREATE TABLE {db}.{table_name}"
        query_response = self.execute_query(sql, response="DF")
        return query_response

    def get_object_details(self, fqtn: str) -> tuple:
        """
        Return details of a table or view in MySQL

        Params:
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)

        Response:
            object exists: bool
            is rasgo object: bool
            object type: [table|view|unknown]
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        db, _, table = parse_fqtn(
            fqtn, default_namespace=self.default_namespace, strict=False
        )
        sql = f"SHOW TABLES IN {db} LIKE '{table}'"
        result = self.execute_query(sql, response="dict")
        obj_exists = len(result) > 0
        is_rasgo_obj = False
        obj_type = "unknown"
        return obj_exists, is_rasgo_obj, obj_type

    # def preview(self, sql: str = None, limit: int = 10) -> pd.DataFrame:
    #     """
    #     Returns 10 records into a pandas DataFrame

    #     Params:
    #     `sql`: str:
    #         SQL statment passed from calling method
    #         This is normally set in the preview method of a transform or dataset,
    #         but must be overridden for MySQL previews because MySQL does not
    #         utliize standard FQTNs
    #     `limit`: int:
    #         Records to return
    #     """
    #     passed_fqtn = sql.split("FROM ")[1]
    #     db, _, table = parse_fqtn(
    #         passed_fqtn, default_namespace=self.default_namespace, strict=False
    #     )
    #     return self.execute_query(
    #         f"SELECT * FROM {db}.{table} LIMIT {limit}",
    #         response="df",
    #         acknowledge_risk=True,
    #     )

    # --------------------------
    # MySQL specific helpers
    # --------------------------
    @property
    def _engine(self) -> "alchemy_engine":
        """
        Returns a SQLAlchemy engine
        """
        engine_url = (
            f"{self.credentials.get('dw_type')}"
            f"+{self.credentials.get('db_api')}://"
            f"{self.credentials.get('username')}:"
            f"{urlquote(self.credentials.get('password'))}"
            f"@{self.credentials.get('host')}/"
            f"{self.credentials.get('database')}"
        )
        print(f"engine_url:\n{engine_url}")
        return alchemy_engine(engine_url)
