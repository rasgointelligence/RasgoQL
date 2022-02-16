"""
Manage Data Warehouse specific imports
"""

try:
    from snowflake import connector as sf_connector
except ImportError:
    sf_connector = None

try:
    from google.cloud import bigquery as bq
except ImportError:
    bq = None

try:
    from google_auth_oauthlib import flow as gcp_flow
except ImportError:
    gcp_flow = None

try:
    from google.oauth2 import service_account as gcp_svc
except ImportError:
    gcp_svc = None

try:
    from google.cloud import exceptions as gcp_exc
except ImportError:
    gcp_exc = None
