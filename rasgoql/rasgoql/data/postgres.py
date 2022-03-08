"""
Snowflake DataWarehouse classes
"""
import logging
import os
from typing import List, Union

import json
import pandas as pd

from rasgoql.data.base import DataWarehouse, DWCredentials
from rasgoql.errors import (
    DWCredentialsWarning, DWConnectionError, DWQueryError,
    PackageDependencyWarning, ParameterException,
    SQLWarning, TableAccessError, TableConflictException
)
from rasgoql.imports import alchemy_engine, alchemy_session, alchemy_exceptions
from rasgoql.primitives.enums import (
    check_response_type, check_table_type, check_write_method
)
from rasgoql.utils.creds import load_env, save_env
from rasgoql.utils.df import cleanse_sql_dataframe, generate_dataframe_ddl
from rasgoql.utils.messaging import verbose_message
from rasgoql.utils.sql import (
    is_scary_sql, magic_fqtn_handler,
    parse_fqtn, parse_table_and_schema_from_fqtn,
    validate_namespace
)

logging.basicConfig()
logger = logging.getLogger('Postgres DataWarehouse')
logger.setLevel(logging.INFO)


class PostgresCredentials(DWCredentials):
    """
    Postgres Credentials
    """
    dw_type = 'postgresql'

    def __init__(
            self,
            username: str,
            password: str,
            host: str,
            port: str,
            database: str,
            schema: str
        ):
        if alchemy_engine is None:
            raise PackageDependencyWarning(
                'Missing a required python package to run Postgres. '
                'Please download the Postgres package by running: '
                'pip install rasgoql[snowflake]')
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
    def from_env(
            cls,
            filepath: str = None
        ) -> 'PostgresCredentials':
        """
        Creates an instance of this Class from a .env file on your machine
        """
        load_env(filepath)
        username = os.getenv('POSTGRES_USERNAME')
        password = os.getenv('POSTGRES_PASSWORD')
        host = os.getenv('POSTGRES_HOST')
        port = os.getenv('POSTGRES_PORT')
        database = os.getenv('POSTGRES_DATABASE')
        schema = os.getenv('POSTGRES_SCHEMA')
        if not all([username, password, host, port, database, schema]):
            raise DWCredentialsWarning(
                'Your env file is missing expected credentials. Consider running '
                'PostgresCredentials(*args).to_env() to repair this.'
            )
        return cls(
            username,
            password,
            host,
            port,
            database,
            schema
        )

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
            "dw_type": self.dw_type
        }

    def to_env(
            self,
            filepath: str = None,
            overwrite: bool = False
        ):
        """
        Saves credentials to a .env file on your machine
        """
        creds = {
            "POSTGRES_USERNAME": self.username,
            "POSTGRES_PASSWORD": self.password,
            "POSTGRES_HOST": self.host,
            "POSTGRES_PORT": self.port,
            "POSTGRES_DATABASE": self.database,
            "POSTGRES_SCHEMA": self.schema
        }
        return save_env(creds, filepath, overwrite)


class PostgresDataWarehouse(DataWarehouse):
    """
    Postgres DataWarehouse
    """
    dw_type = 'postgresql'
    credentials_class = PostgresCredentials

    def __init__(self):
        super().__init__()
        self.credentials: dict = None
        self.connection: alchemy_session = None
        self.database = None
        self.schema = None

    # ---------------------------
    # Core Data Warehouse methods
    # ---------------------------
    def change_namespace(
            self,
            namespace: str
        ) -> None:
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

    def connect(
            self,
            credentials: Union[dict, PostgresCredentials]
        ):
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
            self.database = credentials.get('database')
            self.schema = credentials.get('schema')
            self.connection = alchemy_session(self._engine)
            verbose_message(
                "Connected to Postgres",
                logger
            )
        except Exception as e:
            self._error_handler(e)

    def close_connection(self):
        """
        Close connection to Postgres
        """
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
            verbose_message(
                "Connection to Postgres closed",
                logger
            )
        except Exception as e:
            self._error_handler(e)

    def create(
            self,
            sql: str,
            fqtn: str,
            table_type: str = 'VIEW',
            overwrite: bool = False
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
        schema, table = parse_table_and_schema_from_fqtn(fqtn=fqtn)
        if self._table_exists(fqtn=fqtn) and not overwrite:
            msg = f'A table or view named {fqtn} already exists. ' \
                   'If you are sure you want to overwrite it, ' \
                   'pass in overwrite=True and run this function again'
            raise TableConflictException(msg)
        query = f"CREATE OR REPLACE {table_type} {schema}.{table} AS {sql}"
        self.execute_query(query, acknowledge_risk=True, response='None')
        return fqtn

    @property
    def default_namespace(self) -> str:
        """
        Returns the default database.schema of this connection
        """
        return f'{self.database}.{self.schema}'

    def execute_query(
            self,
            sql: str,
            response: str = 'tuple',
            acknowledge_risk: bool = False
        ):
        """
        Run a query against Postgres and return all results

        `sql`: str:
            query text to execute
        `response`: str:
            Possible values: [dict, df, None]
        `acknowledge_risk`: bool:
            pass True when you know your SQL statement contains
            a potentially dangerous or data-altering operation
            and still want to run it against your DataWarehouse
        """
        response = check_response_type(response)
        if is_scary_sql(sql) and not acknowledge_risk:
            msg = 'It looks like your SQL statement contains a ' \
                  'potentially dangerous or data-altering operation.' \
                  'If you are positive you want to run this, ' \
                  'pass in acknowledge_risk=True and run this function again.'
            raise SQLWarning(msg)
        verbose_message(
            f"Executing query: {sql}",
            logger
        )
        if response == 'DICT':
            return self._query_into_dict(sql)
        if response == 'DF':
            return self._query_into_df(sql)
        return self._execute_string(sql, ignore_results=(response == 'NONE'))

    def get_ddl(
            self,
            fqtn: str
        ) -> pd.DataFrame:
        """
        Returns a DataFrame describing the column in the table

        `fqtn`: str:
            Fully-qualified Table Name (database.schema.table)
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        _, schema_name, table_name = parse_fqtn(fqtn)
        sql = (
            f"select table_schema, table_name, column_name, data_type, "
            f"character_maximum_length, column_default, is_nullable from "
            f"INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}' "
            f"and table_schema = '{schema_name}';"
        )
        query_response = self.execute_query(sql, response='DF')
        return query_response

    def get_object_details(
            self,
            fqtn: str
        ) -> tuple:
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
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        database, schema, table = parse_fqtn(fqtn)
        sql = (
            f"SELECT EXISTS(SELECT FROM pg_catalog.pg_class c JOIN "
            f"pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE "
            f"n.nspname = '{schema}' AND    c.relname = '{table}')"
        )
        result = self.execute_query(sql, response='dict')
        obj_exists = len(result) > 0
        is_rasgo_obj = False
        obj_type = 'unknown'
        return obj_exists, is_rasgo_obj, obj_type

    def get_schema(
            self,
            fqtn: str,
            create_sql: str = None
        ) -> dict:
        """
        Return the schema of a table or view

        Params:
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)
        `create_sql`: str:
            A SQL select statement that will create the view. If this param is passed
            and the fqtn does not already exist, it will be created and profiled based
            on this statement. The view will be dropped after profiling
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        database, schema, table = parse_fqtn(fqtn)
        query_sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES " \
                    f"WHERE table_catalog = '{database}' " \
                    f"AND table_schema = '{schema}' " \
                    f"AND table_name = '{table}'"
        response = []
        try:
            if self._table_exists(fqtn):
                query_response = self.execute_query(query_sql, response='dict')
            elif create_sql:
                self.create(create_sql, fqtn, table_type='view')
                query_response = self.execute_query(query_sql, response='dict')
                self.execute_query(f'DROP VIEW {schema}.{table}', response='none', acknowledge_risk=True)
            else:
                raise TableAccessError(f'Table {fqtn} does not exist or cannot be accessed.')
            for row in query_response:
                response.append((row['name'], row['type']))
            return response
        except Exception as e:
            self._error_handler(e)

    def list_tables(
            self,
            database: str = None,
            schema: str = None
        ) -> pd.DataFrame:
        """
        List all tables and views available in default namespace

        Params:
        `database`: str:
            override database
        `schema`: str:
            override schema
        """
        select_clause = (
            "SELECT TABLE_NAME, "
            "TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME AS FQTN, "
            "CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END AS TABLE_TYPE"
        )
        from_clause = " FROM INFORMATION_SCHEMA.TABLES "
        if database:
            from_clause = f" FROM {database.upper()}.INFORMATION_SCHEMA.TABLES "
        where_clause = f" WHERE TABLE_SCHEMA = '{schema.upper()}'" if schema else ""
        sql = select_clause + from_clause + where_clause
        return self.execute_query(sql, response='df', acknowledge_risk=True)

    def preview(
            self,
            sql: str,
            limit: int = 10
        ) -> pd.DataFrame:
        """
        Returns 10 records into a pandas DataFrame

        Params:
        `sql`: str:
            SQL statment to run
        `limit`: int:
            Records to return
        """
        return self.execute_query(
            f'{sql} LIMIT {limit}',
            response='df',
            acknowledge_risk=True
        )

    def save_df(
            self,
            df: pd.DataFrame,
            fqtn: str,
            method: str = None
        ) -> str:
        """
        Creates a table in Postgres from a pandas Dataframe

        Params:
        `df`: pandas DataFrame:
            DataFrame to upload
        `fqtn`: str:
            Fully-qualied table name (database.schema.table)
            Name for the new table
        `method`: str
            Values: [append, replace]
            when this table already exists in your DataWarehouse,
            pass append: to add dataframe rows to it
            pass replace: to overwrite it with dataframe rows
                WARNING: This will completely overwrite data in the existing table
        """
        if method:
            method = check_write_method(method)
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        database, schema, table = parse_fqtn(fqtn)
        table_exists = self._table_exists(fqtn)
        if table_exists and not method:
            msg = f"A table named {fqtn} already exists. " \
                   "If you are sure you want to write over it, pass in " \
                   "method='append' or method='replace' and run this function again"
            raise TableConflictException(msg)
        try:
            cleanse_sql_dataframe(df)
            # If the table does not exist or we've received instruction to replace
            # Issue a create or replace statement before we insert data
            if not table_exists or method == 'REPLACE':
                create_stmt = generate_dataframe_ddl(df, fqtn)
                self.execute_query(create_stmt, response='None', acknowledge_risk=True)
            df.to_sql(
                table,
                self._engine,
                schema=schema,
                if_exists=method.lower(),
                index=False,
                chunksize=1000
            )
            return fqtn
        except Exception as e:
            self._error_handler(e)

    # ---------------------------
    # Core Data Warehouse helpers
    # ---------------------------
    def _table_exists(
            self,
            fqtn: str
        ) -> bool:
        """
        Check for existence of fqtn in the Data Warehouse and return a boolean

        Params:
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        do_i_exist, _, _ = self.get_object_details(fqtn)
        return do_i_exist

    def _validate_namespace(
            self,
            namespace: str
        ) -> str:
        """
        Checks a namespace string for compliance with Postgres format

        Params:
        `namespace`: str:
            namespace (database.schema)
        """
        try:
            validate_namespace(namespace)
            return namespace.upper()
        except ValueError:
            raise ParameterException("Postgres namespaces should be format: DATABASE.SCHEMA")

    # --------------------------
    # Postgres specific helpers
    # --------------------------
    @property
    def _engine(
            self
        ) -> 'alchemy_engine':
        """
        Returns a SQLAlchemy engine
        """
        engine_url = f"{self.credentials.get('dw_type')}://" \
            f"{self.credentials.get('username')}:" \
            f"{self.credentials.get('password')}" \
            f"@{self.credentials.get('host')}:" \
            f"{self.credentials.get('port')}/" \
            f"{self.credentials.get('database')}"
        return alchemy_engine(engine_url)

    def _error_handler(
            self,
            exception: Exception,
            query: str = None
        ) -> None:
        """
        Handle Postgres exceptions that need additional info
        """
        verbose_message(
            f"Exception occurred while running query: {query}",
            logger
        )
        if exception is None:
            return
        if isinstance(exception, alchemy_exceptions.DisconnectionError):
            raise DWConnectionError(
                'Disconnected from DataWarehouse. Please validate connection '
                'or reconnect.'
            ) from exception
        raise exception

    def _execute_string(
            self,
            query: str,
            ignore_results: bool = False
        ) -> List[tuple]:
        """
        Execute a query string against the DataWarehouse connection and fetch all results
        """
        try:
            results = self.connection.execute(query)
            if ignore_results:
                return
            return list(results)
        except Exception as e:
            self._error_handler(e)

    def _query_into_dict(
            self,
            query: str
        ) -> List[dict]:
        """
        Run a query string and return results in a Snowflake DictCursor

        PRO:
        Results are callable by column name
        for row in data:
            row['COL_NAME']

        CON:
        Query string must be a single statement (only one ;) or Snowflake returns an error
        """
        try:
            query_return = self.connection.execute(query).__dict__
            return query_return
        except Exception as e:
            self._error_handler(e)

    def _query_into_df(
            self,
            query: str
        ) -> pd.DataFrame:
        """
        Run a query string and return results in a pandas DataFrame
        """
        try:
            query_result = self.connection.execute(query)
            query_return_df = pd.DataFrame(query_result.all())
            response_cols = list(query_result.keys())
            query_return_df.columns = response_cols
            return query_return_df
        except Exception as e:
            self._error_handler(e)
