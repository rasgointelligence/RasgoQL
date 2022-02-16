"""
Manage Data Warehouse specific imports
"""

try:
    from snowflake import connector as sf_connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    sf_connector = None
    write_pandas = None

try:
    from google.cloud import bigquery as bq
    from google.cloud import exceptions as gcp_exc
    from google.oauth2 import service_account as gcp_svc
except ImportError:
    bq = None
    gcp_exc = None
    gcp_svc = None

try:
    from google_auth_oauthlib import flow as gcp_flow
except ImportError:
    gcp_flow = None
