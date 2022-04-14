"""
Helpful sql utilities
"""
import random
import re
import string


def cleanse_sql_data_type(
    dtype: str,
) -> str:
    """
    Converts pandas data types to SQL compliant data type
    """
    if dtype.lower() in ["object", "text", "variant"]:
        return "string"
    else:
        return dtype.lower()


def cleanse_sql_name(
    name: str,
) -> str:
    """
    Converts a string to a SQL compliant value
    """
    return name.replace(" ", "_").replace("-", "_").replace('"', '').replace(".", "_")


def is_scary_sql(
    sql: str,
) -> bool:
    """
    Checks a SQL string for presence of injection keywords
    """
    if any(word in sql.upper() for word in SQL_INJECTION_KEYWORDS):
        return True
    return False


def is_restricted_sql(
    sql: str,
) -> bool:
    """
    Checks a SQL string for presence of dangerous keywords
    """
    if any(word in sql.upper() for word in SQL_RESTRICTED_KEYWORDS):
        return True
    return False


def parse_namespace(
    namespace: str,
) -> tuple:
    """
    Accepts a possible namespace string and returns its component parts
    """
    namespace = validate_namespace(namespace)
    return tuple(namespace.split("."))


def random_table_name() -> str:
    """
    Returns a random unique table name prefixed with "RQL"
    """
    return 'RQL_' + ''.join(random.choice(string.ascii_uppercase) for x in range(10))


def validate_namespace(
    namespace: str,
) -> str:
    """
    Accepts a possible namespace string and decides whether it is well-formed
    """
    if re.match(r'^[^\s]+\.[^\s]+', namespace):
        return namespace
    raise ValueError(f'{namespace} is not a well-formed namespace')


def wrap_table(
    parent_table: str,
) -> str:
    """
    Calculates a unique table string
    """
    prefix = 'RQL'
    suffix = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
    if parent_table.startswith('RQL'):
        return f"{parent_table}_{suffix}"
    return f"{prefix}_{parent_table}_{suffix}"


SQL_RESTRICTED_CHARACTERS = [
    ' ',
    '-',
    ';',
]

SQL_INJECTION_KEYWORDS = [
    'DELETE',
    'TRUNCATE',
    'DROP',
    'ALTER',
    'UPDATE',
    'INSERT',
    'MERGE',
]

# fmt: off
SQL_RESTRICTED_KEYWORDS = [
    'ACCOUNT', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'BETWEEN', 'BY',
    'CASE', 'CAST', 'CHECK', 'COLUMN', 'CONNECT', 'CONNECTION', 'CONSTRAINT',
    'CREATE', 'CROSS', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
    'CURRENT_USER', 'DATABASE', 'DELETE', 'DISTINCT', 'DROP', 'ELSE', 'EXISTS', 'FALSE',
    'FOLLOWING', 'FOR', 'FROM', 'FULL', 'GRANT', 'GROUP', 'GSCLUSTER', 'HAVING', 'ILIKE',
    'IN', 'INCREMENT', 'INNER', 'INSERT', 'INTERSECT', 'INTO', 'IS', 'ISSUE', 'JOIN', 'LATERAL',
    'LEFT', 'LIKE', 'LOCALTIME', 'LOCALTIMESTAMP', 'MINUS', 'NATURAL', 'NOT', 'NULL', 'OF', 'ON',
    'OR', 'ORDER', 'ORGANIZATION', 'QUALIFY', 'REGEXP', 'REVOKE', 'RIGHT', 'RLIKE', 'ROW', 'ROWS',
    'SAMPLE', 'SCHEMA', 'SELECT', 'SET', 'SOME', 'START', 'TABLE', 'TABLESAMPLE', 'THEN', 'TO', 'TRIGGER',
    'TRUE', 'TRY_CAST', 'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VALUES', 'VIEW', 'WHEN', 'WHENEVER', 'WHERE', 'WITH'
]
# fmt: on
