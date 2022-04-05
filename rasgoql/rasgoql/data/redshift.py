"""
Redshift DataWarehouse classes
"""
import logging
import os
from typing import List, Optional, Union
import json


import pandas as pd

from rasgoql.data.base import DWCredentials, DataWarehouse

from rasgoql.utils.creds import load_env, save_env

from rasgoql.errors import (
    DWConnectionError,
    DWCredentialsWarning,
    PackageDependencyWarning,
    SQLWarning,
    TableAccessError,
    TableConflictException,
)
from rasgoql.imports import redshift_connector
from rasgoql.primitives.enums import (
    check_response_type, check_write_method, check_table_type
)
from rasgoql.utils.df import cleanse_sql_dataframe, generate_dataframe_ddl
from rasgoql.utils.messaging import verbose_message
from rasgoql.utils.sql import is_scary_sql


logging.basicConfig()
logger = logging.getLogger("Redshift DataWarehouse")
logger.setLevel(logging.INFO)


class RedshiftCredentials(DWCredentials):
    """
    Redshift Credentials
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
        iam: bool = False,
        **kwargs
    ):
        if redshift_connector is None:
            raise PackageDependencyWarning(
                "Missing a required python package to run Redshift. "
                "Please download the Redshift package by running: "
                "pip install rasgoql[redshift]"
            )
        self.username = username
        self.password = password
        self.host = host
        self.port = str(port)
        self.database = database
        self.schema = schema
        self.iam = iam
        self.extra = kwargs

    def __repr__(self) -> str:
        return json.dumps(
            {
                "user": self.username,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "schema": self.schema,
                "iam": self.iam,
                **self.extra
            }
        )

    @classmethod
    def from_env(cls, filepath: Optional[os.PathLike] = None) -> "RedshiftCredentials":
        """
        Creates an instance of this Class from a .env file on your machine
        """
        if filepath: #TODO see if correct behavior
            load_env(filepath)

        env_vars = {var_name.lower()[9:]: value for var_name, value in os.environ.items() if var_name.upper().startswith("REDSHIFT")}

        try:
            username = env_vars.pop("username")
            password = env_vars.pop("password")
            host = env_vars.pop("host")
            port = env_vars.pop("port")
            database = env_vars.pop("database")
            schema = env_vars.pop("schema")
        except KeyError as key_name:
            raise DWCredentialsWarning(
                f"Your env file is missing expected credential variable {key_name}. Consider running "
                "RedshiftCredentials(*args).to_env() to repair this."
            )
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
            "iam": self.iam,
            **self.extra
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
        }
        return save_env(creds, filepath, overwrite)


class RedshiftDataWarehouse(DataWarehouse):
    """
    Redshift DataWarehouse
    """
    dw_type = 'redshift'
    credentials_class = RedshiftCredentials

    def __init__(self):
        super().__init__()
        self.credentials: Optional[dict] = None
        self.connection: redshift_connector.RedshiftConnection = None
        self.default_namespace = None
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
        namespace = self.validate_namespace(namespace)
        database, schema = self.parse_namespace(namespace)
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
            credentials: Union[dict, RedshiftCredentials]
    ):
        """
        Connect to Redshift

        Params:
        `credentials`: dict:
            dict (or RedshiftCredentials class) holding the connection credentials
        """
        if isinstance(credentials, RedshiftCredentials):
            credentials = credentials.to_dict()

        # TODO see if can do this in redshift
        # This allows you to track what queries were run by RasgoQL in your history tab
        credentials.update({"application_name": "rasgoql"})
        #     "session_parameters": {
        #         "QUERY_TAG": "rasgoql"
        #     }
        # })
        try:
            self.credentials = credentials
            self.default_database = credentials.get('database', self.default_database)
            self.default_schema = credentials.get('schema', self.default_schema)
            self.connection = redshift_connector.connect(**credentials)
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
            verbose_message(
                "Connection to Redshift closed",
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
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        if self._table_exists(fqtn) and not overwrite:
            msg = f'A table or view named {fqtn} already exists. ' \
                  'If you are sure you want to overwrite it, ' \
                  'pass in overwrite=True and run this function again'
            raise TableConflictException(msg)
        query = f"CREATE OR REPLACE {table_type} {fqtn} AS {sql}"
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
        namespace = self.validate_namespace(new_namespace)
        db, schema = self.parse_namespace(namespace)
        self.default_database = db
        self.default_schema = schema

    def execute_query(
            self,
            sql: str,
            response: str = 'tuple',
            acknowledge_risk: bool = False,
            batches: bool = False
    ):
        """
        Run a query against Redshift and return all results

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
            return self._execute_df_cursor(sql, batches=batches)
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
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        sql = f"SELECT GET_DDL('TABLE', '{fqtn}') AS DDL"
        query_response = self.execute_query(sql, response='dict')
        return query_response[0]['DDL']

    def get_object_details(
            self,
            fqtn: str
    ) -> tuple:
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
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
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
        Creates a table in Redshift from a pandas Dataframe

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
        fqtn = self.magic_fqtn_handler(fqtn, self.default_namespace)
        do_i_exist, _, _ = self.get_object_details(fqtn)
        return do_i_exist

    # --------------------------
    # Redshift specific helpers
    # --------------------------
    def parse_table_and_schema_from_fqtn(
        self,
        fqtn: str
    ) -> tuple:
        """
        Accepts a possible FQTN and returns the schema and table from it
        """
        fqtn = self.validate_fqtn(fqtn)
        return tuple(fqtn.split(".")[1:])

    def _error_handler(
            self,
            exception: Exception,
            query: str = None
    ) -> None:
        """
        Handle Redshift exceptions that need additional info
        """
        verbose_message(
            f"Exception occurred while running query: {query}",
            logger
        )
        if exception is None:
            return
        if isinstance(exception, redshift_connector.error.ProgrammingError):
            if exception.errno == 3001:
                raise TableAccessError(
                    'You do not have access to operate on this object. '
                    'Two possible ways to resolve: '
                    'Connect with different credentials that have the proper access. '
                    'Or run `.change_namespace` on your SQLChain to write it to a '
                    'namespace your credentials can access'
                ) from exception
        if isinstance(exception, redshift_connector.errors.DatabaseError):
            if exception.errno == 250001:
                raise DWConnectionError(
                    'Invalid username / password, please check that your '
                    'credentials are correct and try to reconnect.'
                ) from exception
        if isinstance(exception, redshift_connector.errors.ServiceUnavailableError):
            raise DWConnectionError(
                'Redshift is unavailable. Please check that your are using '
                'a valid account identifier, that you have internet access, and '
                'that http connections to Redshift are whitelisted in your env. '
                'Finally check https://status.redshift.com/ for outage status.'
            ) from exception
        raise exception

    def _execute_dict_cursor(
            self,
            query: str
    ) -> List[dict]:
        """
        Run a query string and return results in a Redshift DictCursor

        PRO:
        Results are callable by column name
        for row in data:
            row['COL_NAME']

        CON:
        Query string must be a single statement (only one ;) or Redshift returns an error
        """
        cursor = None
        try:
            cursor = self.connection.cursor()
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
            params: Optional[dict] = None,
            batches: Optional[bool] = False
    ) -> pd.DataFrame:
        """
        Run a query string and return results in a pandas DataFrame
        """
        cursor = None
        try:
            cursor = self.connection
            cursor.execute(query, params)
            if batches:
                return cursor.fetch_pandas_batches()
            return cursor.fetch_pandas_all()
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
