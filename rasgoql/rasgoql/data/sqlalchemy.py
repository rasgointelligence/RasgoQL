"""
Generic SQLAlchemy DataWarehouse classes
"""
from __future__ import annotations
from abc import abstractmethod
import logging
import re
from typing import Optional, Union
from urllib.parse import quote_plus as urlquote

import pandas as pd

from rasgoql.data.base import DataWarehouse, DWCredentials
from rasgoql.errors import (
    DWConnectionError,
    ParameterException,
    SQLWarning,
    TableAccessError,
    TableConflictException,
)
from rasgoql.imports import alchemy_engine, alchemy_session, alchemy_exceptions
from rasgoql.primitives.enums import (
    check_response_type,
    check_table_type,
    check_write_method,
)
from rasgoql.utils.df import cleanse_sql_dataframe, generate_dataframe_ddl
from rasgoql.utils.messaging import verbose_message
from rasgoql.utils.sql import is_scary_sql

logging.basicConfig()
logger = logging.getLogger("SQLAlchemy DataWarehouse")
logger.setLevel(logging.INFO)

# Each DB derived from the generic SQLAlchemy class will have slightly
# different connection strings. Build a unique Credentials class for each DB


class SQLAlchemyDataWarehouse(DataWarehouse):
    """
    Base SQLAlchemy DataWarehouse
    """

    dw_type = None
    credentials_class = None

    def __init__(self):
        super().__init__()
        self.credentials: Optional[Union[dict, DWCredentials]]
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
            "Connecting to a new Database in a single session is not supported "
            "by the SQLAlchemy Engine. Please build a new connection."
        )


    @abstractmethod
    def connect(self, credentials: Union[dict, DWCredentials]):
        """
        Connect to DB
        Params:
        `credentials`: dict:
            dict  holding the connection credentials
        """
        if isinstance(credentials, DWCredentials):
            credentials = credentials.to_dict()

        try:
            self.credentials = credentials
            self.database = credentials.get("database")
            self.schema = credentials.get("schema")
            self.connection = alchemy_session(self._engine, autocommit=True)
        except Exception as e:
            self._error_handler(e)

    def close_connection(self):
        """
        Close connection
        """
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
            verbose_message("Connection closed", logger)
        except Exception as e:
            self._error_handler(e)

    def create(
        self, sql: str, fqtn: str, table_type: str = "VIEW", overwrite: bool = False
    ):
        """
        Create a view or table from given SQL
        If a derived class's DB cannot execute against a FQTN (e.g., Postgres),
        override this method or SQL execution will fail
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

    @property
    def default_namespace(self) -> str:
        """
        Returns the default database.schema of this connection
        """
        return f"{self.database}.{self.schema}"

    @default_namespace.setter
    def default_namespace(self, new_namespace: str):
        namespace = self._validate_namespace(new_namespace)
        db, schema = self.parse_namespace(namespace)
        self.default_database = db
        self.default_schema = schema

    def execute_query(
        self,
        sql: str,
        response: str = "tuple",
        acknowledge_risk: bool = False,
        **kwargs
    ) -> Union[list[dict], pd.DataFrame, list[tuple], None]:
        """
        Run a query against DB and return all results
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
            msg = (
                "It looks like your SQL statement contains a "
                "potentially dangerous or data-altering operation."
                "If you are positive you want to run this, "
                "pass in acknowledge_risk=True and run this function again."
            )
            raise SQLWarning(msg)
        verbose_message(f"Executing query: {sql}", logger)
        if response == "DICT":
            return self._query_into_dict(sql)
        if response == "DF":
            return self._query_into_df(sql)
        return self._execute_string(sql, ignore_results=(response == "NONE"))

    @abstractmethod
    def get_ddl(self, fqtn: str) -> pd.DataFrame:
        """
        Returns a DataFrame describing the column in the table
        This method should be overridden by derived classes since optimal DDL
        selection will vary by DB type
        `fqtn`: str:
            Fully-qualified Table Name (database.schema.table)
        """
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        db_name, schema_name, table_name = self.parse_fqtn(fqtn)
        sql = (
            f"select table_schema, table_name, column_name, data_type, "
            f"character_maximum_length, column_default, is_nullable from "
            f"INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}' "
            f"and table_schema = '{schema_name}';"
        )
        query_response = self.execute_query(sql, response="DF")
        return query_response

    @abstractmethod
    def get_object_details(self, fqtn: str) -> tuple:
        """
        Return details of a table or view
        This method should be overridden by derived classes since optimal DDL
        selection will vary by DB type
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
        sql = f"SHOW OBJECTS LIKE '{table}' IN {database}.{schema}"
        result = self.execute_query(sql, response="dict")
        obj_exists = len(result) > 0
        is_rasgo_obj = False
        obj_type = "unknown"
        if obj_exists:
            is_rasgo_obj = result[0].get("comment") == "rasgoql"
            obj_type = result[0].get("kind")
        return obj_exists, is_rasgo_obj, obj_type

    def get_schema(self, fqtn: str, create_sql: str = None) -> dict:
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
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        database, schema, table = self.parse_fqtn(fqtn)
        query_sql = (
            f"SELECT * FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE table_catalog = '{database}' "
            f"AND table_schema = '{schema}' "
            f"AND table_name = '{table}'"
        )
        response = []
        try:
            if self._table_exists(fqtn):
                query_response = self.execute_query(query_sql, response="dict")
            elif create_sql:
                self.create(create_sql, fqtn, table_type="view")
                query_response = self.execute_query(query_sql, response="dict")
                self.execute_query(
                    f"DROP VIEW {schema}.{table}",
                    response="none",
                    acknowledge_risk=True,
                )
            else:
                raise TableAccessError(
                    f"Table {fqtn} does not exist or cannot be accessed."
                )
            for row in query_response:
                response.append((row["name"], row["type"]))
            return response
        except Exception as e:
            self._error_handler(e)

    def list_tables(self, database: str = None, schema: str = None) -> pd.DataFrame:
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
        return self.execute_query(sql, response="df", acknowledge_risk=True)

    def preview(self, sql: str, limit: int = 10) -> pd.DataFrame:
        """
        Returns 10 records into a pandas DataFrame
        Params:
        `sql`: str:
            SQL statment to run
        `limit`: int:
            Records to return
        """
        return self.execute_query(
            f"{sql} LIMIT {limit}", response="df", acknowledge_risk=True
        )

    def save_df(self, df: pd.DataFrame, fqtn: str, method: str = None) -> str:
        """
        Creates a table from a pandas Dataframe
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
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        database, schema, table = self.parse_fqtn(fqtn)
        table_exists = self._table_exists(fqtn)
        if table_exists and not method:
            msg = (
                f"A table named {fqtn} already exists. "
                "If you are sure you want to write over it, pass in "
                "method='append' or method='replace' and run this function again"
            )
            raise TableConflictException(msg)
        try:
            cleanse_sql_dataframe(df)
            # If the table does not exist or we've received instruction to replace
            # Issue a create or replace statement before we insert data
            if not table_exists or method == "REPLACE":
                create_stmt = generate_dataframe_ddl(df, fqtn)
                self.execute_query(create_stmt, response="None", acknowledge_risk=True)
            df.to_sql(
                table,
                self._engine,
                schema=schema,
                if_exists=method.lower(),
                index=False,
                chunksize=1000,
            )
            return fqtn
        except Exception as e:
            self._error_handler(e)

    # ---------------------------
    # Core Data Warehouse helpers
    # ---------------------------
    def _table_exists(self, fqtn: str) -> bool:
        """
        Check for existence of fqtn in the Data Warehouse and return a boolean
        Params:
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)
        """
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        do_i_exist, _, _ = self.get_object_details(fqtn)
        return do_i_exist

    # --------------------------
    # SQLAlchemy and derived class helpers
    # --------------------------
    @property
    @abstractmethod
    def _engine(self):
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

    def _error_handler(self, exception: Exception, query: str = None) -> None:
        """
        Handle SQLAlchemy exceptions that need additional info
        """
        verbose_message(f"Exception occurred while running query: {query}", logger)
        if exception is None:
            return
        if isinstance(exception, alchemy_exceptions.DisconnectionError):
            raise DWConnectionError(
                "Disconnected from DataWarehouse. Please validate connection "
                "or reconnect."
            ) from exception
        raise exception

    def _execute_string(self, query: str, ignore_results: bool = False) -> list[tuple]:
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

    def _query_into_dict(self, query: str) -> list[dict]:
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

    def _query_into_df(self, query: str) -> pd.DataFrame:
        """
        Run
        """
        try:
            query_result = self.connection.execute(query)
            query_return_df = pd.DataFrame(query_result.all())
            response_cols = list(query_result.keys())
            query_return_df.columns = response_cols
            return query_return_df
        except Exception as e:
            self._error_handler(e)
