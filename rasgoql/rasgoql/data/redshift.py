"""
Redshift DataWarehouse classes
"""
from __future__ import annotations
import logging
import os
from typing import Optional, Union
import json

import pandas as pd

from rasgoql.data.base import DWCredentials
from rasgoql.data.sqlalchemy import SQLAlchemyDataWarehouse
from rasgoql.imports import alchemy_engine, alchemy_session, alchemy_url
from rasgoql.utils.creds import load_env, save_env
from rasgoql.errors import DWCredentialsWarning, TableConflictException
from rasgoql.primitives.enums import check_table_type
from rasgoql.utils.messaging import verbose_message


logging.basicConfig()
logger = logging.getLogger("Redshift DataWarehouse")
logger.setLevel(logging.INFO)


class RedshiftCredentials(DWCredentials):
    """
    Redshift Credentials

    Full list of accepted parameters available at
    https://github.com/aws/amazon-redshift-python-driver#connection-parameters
    """

    dw_type = "redshift"

    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: Union[str, int],
        database: str,
        schema: str,
        **kwargs,
    ):
        self.username = username
        self.password = password
        self.host = host
        try:
            self.port = int(port)
        except ValueError:
            raise DWCredentialsWarning(f"Redshift port number must be an integer") from None
        self.database = database
        self.schema = schema
        self.conn_params = kwargs

    def __repr__(self) -> str:
        return json.dumps(
            {
                "username": self.username,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "schema": self.schema,
                "conn_params": self.conn_params,
            }
        )

    @classmethod
    def from_env(cls, filepath: Optional[os.PathLike] = None) -> RedshiftCredentials:
        """
        Creates an instance of this Class from a .env file on your machine
        """
        load_env(filepath)
        env_vars = cls._parse_env_vars("REDSHIFT_")

        try:
            username = env_vars.pop("user" if "user" in env_vars else "username")
            password = env_vars.pop("password")
            host = env_vars.pop("host")
            port = int(env_vars.pop("port"))
            database = env_vars.pop("database")
            schema = env_vars.pop("schema")
        except KeyError as key_name:
            raise DWCredentialsWarning(
                f"Your env file is missing expected credential variable {key_name}. Consider running "
                "RedshiftCredentials(*args).to_env() to repair this.") from None
        except ValueError:
            raise DWCredentialsWarning(f"Redshift port number must be an integer") from None
        return cls(username, password, host, port, database, schema, **env_vars)

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
            "conn_params": self.conn_params,
        }

    def to_env(self, filepath: str = None, overwrite: bool = False):
        """
        Saves credentials to a .env file on your machine
        """
        creds = {
            "REDSHIFT_USERNAME": self.username,
            "REDSHIFT_PASSWORD": self.password,
            "REDSHIFT_HOST": self.host,
            "REDSHIFT_PORT": self.port,
            "REDSHIFT_DATABASE": self.database,
            "REDSHIFT_SCHEMA": self.schema,
            **{f"REDSHIFT_{key}": str(value) for key, value in self.conn_params.items()},
        }
        return save_env(creds, filepath, overwrite)


class RedshiftDataWarehouse(SQLAlchemyDataWarehouse):
    """
    Redshift DataWarehouse
    """

    dw_type = "redshift"
    credentials_class = RedshiftCredentials

    def __init__(self):
        super().__init__()
        self.credentials: dict
        self.connection: alchemy_session
        self.database = None
        self.schema = None

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
            "Connecting to a new Database in a single session is not supported by Redshift. "
            "Please build a new connection using the RedshiftCredentials class"
        )

    def connect(self, credentials: Union[dict, RedshiftCredentials]):
        """
        Connect to Redshift

        Params:
        `credentials`: dict:
            dict (or RedshiftCredentials class) holding the connection credentials
        """
        if isinstance(credentials, RedshiftCredentials):
            credentials = credentials.to_dict()

        # This allows you to track what queries were run by RasgoQL in your history tab
        try:
            credentials["conn_params"].update({"application_name": "rasgoql"})
        except KeyError:
            credentials["conn_params"] = {"application_name": "rasgoql"}

        try:
            self.credentials = credentials
            self.database = credentials.get("database")
            self.schema = credentials.get("schema")
            self.connection = alchemy_session(self._engine)
            verbose_message("Connected to Redshift", logger)
        except Exception as e:
            self._error_handler(e)

    def close_connection(self):
        """
        Close connection to Redshift
        """
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
            verbose_message("Connection to Redshift closed", logger)
        except Exception as e:
            self._error_handler(e)

    def create(self, sql: str, fqtn: str, table_type: str = "VIEW", overwrite: bool = False):
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
        _, schema_name, table_name = self.parse_fqtn(fqtn)
        if self._table_exists(fqtn) and not overwrite:
            msg = (
                f"A table or view named {fqtn} already exists. "
                "If you are sure you want to overwrite it, "
                "pass in overwrite=True and run this function again"
            )
            raise TableConflictException(msg)
        query = f"CREATE OR REPLACE {table_type} {schema_name}.{table_name} AS {sql}"
        self.execute_query(query, acknowledge_risk=True, response="None")
        return fqtn

    @property
    def default_namespace(self) -> str:
        """
        Returns the default database.schema of this connection
        """
        return f"{self.database}.{self.schema}"

    @default_namespace.setter
    def default_namespace(self, new_namespace: str):
        """
        Setter method for the `default_namespace` property
        """
        namespace = self.validate_namespace(new_namespace)
        db, schema = self.parse_namespace(namespace)
        self.database = db
        self.schema = schema

    def get_ddl(self, fqtn: str) -> pd.DataFrame:
        return super().get_ddl(fqtn)

    def get_object_details(self, fqtn: str) -> tuple:
        """
        Return details of a table or view in Redshift
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
            f"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN "
            f"pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE "
            f"n.nspname = '{schema}' AND c.relname = '{table}')"
        )
        result = self.execute_query(sql, response="dict")
        obj_exists = len(result) > 0
        is_rasgo_obj = False
        obj_type = "unknown"
        return obj_exists, is_rasgo_obj, obj_type

    # --------------------------
    # Redshift specific helpers
    # --------------------------
    @property
    def _engine(self) -> alchemy_engine:
        """
        Returns a SQLAlchemy engine
        """
        url = alchemy_url.create(
            drivername="redshift+redshift_connector",
            database=self.database,
            username=self.credentials.get("username"),
            password=self.credentials.get("password"),
            host=self.credentials.get("host"),
            port=self.credentials.get("port"),
        )
        return alchemy_engine(
            url,
            connect_args=self.credentials.get("conn_params", {}),
        )
