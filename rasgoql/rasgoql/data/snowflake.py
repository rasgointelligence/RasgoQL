"""
Snowflake DataWarehouse classes
"""
import logging
import os
from typing import List, Optional, Union

import json
import pandas as pd

from rasgoql.data.base import DataWarehouse, DWCredentials
from rasgoql.errors import (
    DWCredentialsWarning, DWConnectionError, DWQueryError,
    PackageDependencyWarning, ParameterException,
    SQLWarning, TableAccessError, TableConflictException
)
from rasgoql.imports import sf_connector, write_pandas
from rasgoql.primitives.enums import (
    check_response_type, check_write_method, check_write_table_type
)
from rasgoql.utils.creds import load_env, save_env
from rasgoql.utils.df import cleanse_sql_dataframe, generate_dataframe_ddl
from rasgoql.utils.messaging import verbose_message
from rasgoql.utils.sql import (
    is_scary_sql, magic_fqtn_handler,
    parse_fqtn, parse_namespace,
    validate_namespace
)

logging.basicConfig()
logger = logging.getLogger('Snowflake DataWarehouse')
logger.setLevel(logging.INFO)


class SnowflakeCredentials(DWCredentials):
    """
    Snowflake Credentials
    """
    dw_type = 'snowflake'

    def __init__(
            self,
            account: str,
            user: str,
            password: str,
            role: str,
            warehouse: str,
            database: str,
            schema: str
        ):
        if sf_connector is None:
            raise PackageDependencyWarning(
                'Missing a required python package to run Snowflake. '
                'Please download the Snowflake package by running: '
                'pip install rasgoql[snowflake]')
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def __repr__(self) -> str:
        return json.dumps(
            {
                "account": self.account,
                "user": self.user,
                "role": self.role,
                "database": self.database,
                "schema": self.schema,
                "warehouse": self.warehouse,
            }
        )

    @classmethod
    def from_env(
            cls,
            filepath: str = None
        ) -> 'SnowflakeCredentials':
        """
        Creates an instance of this Class from a .env file on your machine
        """
        load_env(filepath)
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        role = os.getenv('SNOWFLAKE_ROLE')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE')
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        if not all([account, user, password, role, warehouse, database, schema]):
            raise DWCredentialsWarning(
                'Your env file is missing expected credentials. Consider running '
                'SnowflakeCredentials(*args).to_env() to repair this.'
            )
        return cls(
            account,
            user,
            password,
            role,
            warehouse,
            database,
            schema
        )

    def to_dict(self) -> dict:
        """
        Returns a dict of the credentials
        """
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "role": self.role,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema
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
            "SNOWFLAKE_ACCOUNT": self.account,
            "SNOWFLAKE_USER": self.user,
            "SNOWFLAKE_PASSWORD": self.password,
            "SNOWFLAKE_ROLE": self.role,
            "SNOWFLAKE_WAREHOUSE": self.warehouse,
            "SNOWFLAKE_DATABASE": self.database,
            "SNOWFLAKE_SCHEMA": self.schema,
        }
        return save_env(creds, filepath, overwrite)


class SnowflakeDataWarehouse(DataWarehouse):
    """
    Snowflake DataWarehouse
    """
    dw_type = 'snowflake'
    credentials_class = SnowflakeCredentials

    def __init__(self):
        super().__init__()
        self.credentials: dict = None
        self.connection: sf_connector.SnowflakeConnection = None
        self.default_database = None
        self.default_schema = None

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
        namespace = self._validate_namespace(namespace)
        database, schema = parse_namespace(namespace)
        try:
            self.execute_query(f'USE DATABASE {database}')
            self.execute_query(f'USE SCHEMA {schema}')
            self.default_namespace = namespace
            self.default_database = database
            self.default_schema = schema
            verbose_message(
                f"Namespace reset to {self.default_namespace}",
                logger
            )
        except Exception as e:
            self._error_handler(e)

    def connect(
            self,
            credentials: Union[dict, SnowflakeCredentials]
        ):
        """
        Connect to Snowflake

        Params:
        `credentials`: dict:
            dict (or DWCredentials class) holding the connection credentials
        """
        if isinstance(credentials, SnowflakeCredentials):
            credentials = credentials.to_dict()

        # This allows you to track what queries were run by RasgoQL in your history tab
        credentials.update({
            "application": "rasgoql",
            "session_parameters": {
                "QUERY_TAG": "rasgoql"
            }
        })
        try:
            self.credentials = credentials
            self.default_database = credentials.get('database')
            self.default_schema = credentials.get('schema')
            self.connection = sf_connector.connect(**credentials)
            verbose_message(
                "Connected to Snowflake",
                logger
            )
        except Exception as e:
            self._error_handler(e)

    def close_connection(self):
        """
        Close connection to Snowflake
        """
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
            verbose_message(
                "Connection to Snowflake closed",
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
        table_type = check_write_table_type(table_type)
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        if self._table_exists(fqtn) and not overwrite:
            msg = f'A table or view named {fqtn} already exists. ' \
                   'If you are sure you want to overwrite it, ' \
                   'pass in overwrite=True and run this function again'
            raise TableConflictException(msg)
        query = f"CREATE OR REPLACE {table_type} {fqtn} COMMENT='rasgoql' AS {sql}"
        self.execute_query(query, acknowledge_risk=True, response='None')
        return fqtn

    @property
    def default_namespace(self) -> str:
        """
        Returns the default database.schema of this connection
        """
        return f'{self.default_database}.{self.default_schema}'

    @default_namespace.setter
    def default_namespace(
        self,
        new_namespace: str
    ):
        """
        Setter method for the `default_namespace` property
        """
        namespace = self._validate_namespace(new_namespace)
        db, schema = parse_namespace(namespace)
        self.default_database = db
        self.default_schema = schema
        

    def execute_query(
            self,
            sql: str,
            response: str = 'tuple',
            acknowledge_risk: bool = False
        ):
        """
        Run a query against Snowflake and return all results

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
            return self._execute_dict_cursor(sql)
        if response == 'DF':
            return self._execute_df_cursor(sql)
        return self._execute_string(sql, ignore_results=(response == 'NONE'))

    def get_ddl(
            self,
            fqtn: str
        ) -> str:
        """
        Returns the create statement for a table or view

        `fqtn`: str:
            Fully-qualified Table Name (database.schema.table)
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_namespace)
        sql = f"SELECT GET_DDL('TABLE', '{fqtn}') AS DDL"
        query_response = self.execute_query(sql, response='dict')
        return query_response[0]['DDL']

    def get_object_details(
            self,
            fqtn: str
        ) -> tuple:
        """
        Return details of a table or view in Snowflake

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
        sql = f"SHOW OBJECTS LIKE '{table}' IN {database}.{schema}"
        result = self.execute_query(sql, response='dict')
        obj_exists = len(result) > 0
        is_rasgo_obj = False
        obj_type = 'unknown'
        if obj_exists:
            is_rasgo_obj = (result[0].get('comment') == 'rasgoql')
            obj_type = result[0].get('kind')
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
        desc_sql = f"DESC TABLE {fqtn}"
        response = []
        try:
            if self._table_exists(fqtn):
                query_response = self.execute_query(desc_sql, response='dict')
            elif create_sql:
                self.create(create_sql, fqtn, table_type='view')
                query_response = self.execute_query(desc_sql, response='dict')
                self.execute_query(f'DROP VIEW {fqtn}', response='none', acknowledge_risk=True)
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
        select_clause = "SELECT TABLE_NAME, " \
                        "TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME AS FQTN, " \
                        "CASE TABLE_TYPE WHEN 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END AS TABLE_TYPE, " \
                        "ROW_COUNT, CREATED, LAST_ALTERED "
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
        Creates a table in Snowflake from a pandas Dataframe

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
                create_stmt += " COMMENT='rasgoql' "
                self.execute_query(create_stmt, response='None', acknowledge_risk=True)
            _success, _chunks, _rows, _output = write_pandas(
                conn=self.connection,
                df=df,
                table_name=fqtn,
                quote_identifiers=False
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
        Checks a namespace string for compliance with Snowflake format

        Params:
        `namespace`: str:
            namespace (database.schema)
        """
        try:
            validate_namespace(namespace)
            return namespace.upper()
        except ValueError:
            raise ParameterException("Snowflake namespaces should be format: DATABASE.SCHEMA")

    # --------------------------
    # Snowflake specific helpers
    # --------------------------
    def _error_handler(
            self,
            exception: Exception,
            query: str = None
        ) -> None:
        """
        Handle Snowflake exceptions that need additional info
        """
        verbose_message(
            f"Exception occurred while running query: {query}",
            logger
        )
        if exception is None:
            return
        if isinstance(exception, sf_connector.errors.ProgrammingError):
            if exception.errno == 3001:
                raise TableAccessError(
                    'You do not have access to operate on this object. '
                    'Two possible ways to resolve: '
                    'Connect with different credentials that have the proper access. '
                    'Or run `.change_namespace` on your SQLChain to write it to a '
                    'namespace your credentials can access'
                ) from exception
        if isinstance(exception, sf_connector.errors.DatabaseError):
            if exception.errno == 250001:
                raise DWConnectionError(
                    'Invalid username / password, please check that your '
                    'credentials are correct and try to reconnect.'
                ) from exception
        if isinstance(exception, sf_connector.errors.ServiceUnavailableError):
            raise DWConnectionError(
                'Snowflake is unavailable. Please check that your are using '
                'a valid account identifier, that you have internet access, and '
                'that http connections to Snowflake are whitelisted in your env. '
                'Finally check https://status.snowflake.com/ for outage status.'
            ) from exception
        raise exception

    def _execute_dict_cursor(
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
        cursor = None
        try:
            cursor = self.connection.cursor(sf_connector.DictCursor)
            query_return = cursor.execute(query).fetchall()
            return query_return
        except Exception as e:
            self._error_handler(e)
        finally:
            if cursor:
                cursor.close()

    def _execute_df_cursor(
            self,
            query: str,
            params: Optional[dict] = None
        ) -> pd.DataFrame:
        """
        Run a query string and return results in a pandas DataFrame
        """
        cursor = None
        try:
            cursor = self.connection.cursor()
            query_return = cursor.execute(query, params).fetch_pandas_all()
            return query_return
        except Exception as e:
            self._error_handler(e)
        finally:
            if cursor:
                cursor.close()

    def _execute_string(
            self,
            query: str,
            ignore_results: bool = False
        ) -> List[tuple]:
        """
        Execute a query string against the Data Warehouse connection and fetch all results
        """
        query_returns = []
        cursor = None
        try:
            for cursor in self.connection.execute_string(query, return_cursors=(not ignore_results)):
                for query_return in cursor:
                    query_returns.append(query_return)
            return query_returns
        except Exception as e:
            self._error_handler(e)
        finally:
            if cursor:
                cursor.close()
