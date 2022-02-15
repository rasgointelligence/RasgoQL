"""
Snowflake DataWarehouse classes
"""
import logging
import os
import re
from typing import List, Optional, Union

import json
import pandas as pd
from snowflake import connector
from snowflake.connector.pandas_tools import write_pandas

from rasgoql.errors import (
    DWConnectionError, DWQueryError,
    ParameterException, SQLException,
    TableAccessError, TableConflictException
)
from rasgoql.primitives.enums import (
    check_response_type, check_table_type, check_write_method
)
from rasgoql.utils.creds import load_env, save_env
from rasgoql.utils.df import cleanse_sql_dataframe, generate_dataframe_ddl
from rasgoql.utils.sql import is_scary_sql, magic_fqtn_handler, parse_fqtn

from .base import DataWarehouse, DWCredentials

logging.basicConfig()
logger = logging.getLogger(__name__)
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
                "warehouse": self.warehouse,
            }
        )

    @classmethod
    def from_env(
            cls,
            file_path: str = None
        ) -> 'SnowflakeCredentials':
        """
        Creates an instance of this Class from a .env file on your machine
        """
        load_env(file_path)
        return cls(
            os.getenv('snowflake_account'),
            os.getenv('snowflake_user'),
            os.getenv('snowflake_password'),
            os.getenv('snowflake_role'),
            os.getenv('snowflake_warehouse'),
            os.getenv('snowflake_database'),
            os.getenv('snowflake_schema')
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
            file_path: str = None,
            overwrite: bool = False
        ):
        """
        Saves credentials to a .env file on your machine
        """
        creds = f'snowflake_account={self.account}\n'
        creds += f'snowflake_user={self.user}\n'
        creds += f'snowflake_password={self.password}\n'
        creds += f'snowflake_role={self.role}\n'
        creds += f'snowflake_warehouse={self.warehouse}\n'
        creds += f'snowflake_database={self.database}\n'
        creds += f'snowflake_schema={self.schema}\n'
        return save_env(creds, file_path, overwrite)


class SnowflakeDataWarehouse(DataWarehouse):
    """
    Snowflake DataWarehouse
    """
    dw_type = 'snowflake'
    credentials_class = SnowflakeCredentials

    def __init__(self):
        super().__init__()
        self.credentials: dict = None
        self.connection: connector.SnowflakeConnection = None
        self.default_database = None
        self.default_schema = None

    # ---------------------------
    # Core Data Warehouse methods
    # ---------------------------

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
            self.connection = connector.connect(**credentials)
        except connector.errors.DatabaseError as e:
            raise DWConnectionError(e)
        except connector.errors.ForbiddenError as e:
            raise DWConnectionError(e)
        except Exception as e:
            raise e

    def close_connection(self):
        """
        Close connection to Snowflake
        """
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
            logger.info("Connection to Snowflake closed")
        except connector.errors.DatabaseError as e:
            raise DWConnectionError(e)
        except Exception as e:
            raise e

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
        fqtn = magic_fqtn_handler(fqtn, self.default_database, self.default_schema)
        if self._table_exists(fqtn) and not overwrite:
            msg = f'A table or view named {fqtn} already exists. ' \
                   'If you are sure you want to overwrite it, ' \
                   'pass in overwrite=True and run this function again'
            raise TableConflictException(msg)
        query = f'CREATE OR REPLACE {table_type} {fqtn} AS {sql}'
        self.execute_query(query, acknowledge_risk=True, response='None')
        return fqtn

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
            raise SQLException(msg)
        logger.debug('>>>Executing SQL:')
        logger.debug(sql)
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
        fqtn = magic_fqtn_handler(fqtn, self.default_database, self.default_schema)
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
        fqtn = magic_fqtn_handler(fqtn, self.default_database, self.default_schema)
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
            fqtn: str
        ) -> dict:
        """
        Return the schema of a table or view

        Params:
        `fqtn`: str:
            Fully-qualified table name (database.schema.table)
        """
        fqtn = magic_fqtn_handler(fqtn, self.default_database, self.default_schema)
        sql = f"DESC TABLE {fqtn}"
        query_response = self.execute_query(sql, response='dict')
        return query_response

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
        return self.execute_query(f'{sql} LIMIT {limit}', response='df', acknowledge_risk=True)

    def save_df(
            self,
            df: pd.DataFrame,
            fqtn: str,
            method: str = None
        ):
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
        fqtn = magic_fqtn_handler(fqtn, self.default_database, self.default_schema)
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
            success, chunks, rows, output = write_pandas(
                conn=self.connection,
                df=df,
                table_name=fqtn,
                quote_identifiers=False
            )
            return success, chunks, rows, output
        except Exception as e:
            raise e

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
        fqtn = magic_fqtn_handler(fqtn, self.default_database, self.default_schema)
        do_i_exist, _, _ = self.get_object_details(fqtn)
        return do_i_exist

    def _validate_namespace(
            self,
            namespace: str
        ):
        """
        Checks a namespace string for compliance with Snowflake format

        Params:
        `namespace`: str:
            namespace (database.schema.table)
        """
        # Does this match a 'string.string' pattern?
        if re.match(r'\w+\.\w+', namespace):
            return
        raise ParameterException("Snowflake namespaces should be format: DATABASE.SCHEMA")

    # --------------------------
    # Snowflake specific helpers
    # --------------------------
    def _execute_string(
            self,
            query: str, *,
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
        except connector.errors.ProgrammingError as e:
            raise DWQueryError(e)
        except Exception as e:
            raise e
        finally:
            if cursor:
                cursor.close()

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
            cursor = self.connection.cursor(connector.DictCursor)
            query_return = cursor.execute(query).fetchall()
            return query_return
        except connector.errors.ProgrammingError as e:
            raise DWQueryError(e)
        except Exception as e:
            raise e
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
        except connector.errors.ProgrammingError as e:
            raise DWQueryError(e)
        except Exception as e:
            raise e
        finally:
            if cursor:
                cursor.close()
