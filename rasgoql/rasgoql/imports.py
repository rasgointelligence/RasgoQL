"""
Manage Data Warehouse specific imports
"""
import logging
import pkg_resources
import requests

logging.basicConfig()
logger = logging.getLogger('RasgoQL Dependency Warning')
logger.setLevel(logging.INFO)

### ------------
# Note to users:
# This module is used to allow rasgoql to install the minimum dependencies
# needed to run your flavor of DataWarehouse

# We support downloading extas packages for each DW: e.g. `rasgoql[snowflake]`
# so your machine is not cluttered with unused google or sqlalchemy packages.
# In doing so, we run the risk of not having required dependencies at runtime

# This module attempts to import all possible datawarehouse packages, and
# fails gracefully if they are not present
# RasgoQL will use these empty aliases to raise a PackageDependencyWarning
# from the class or function that needs the import
### ------------

# Check for latest version of rasgotransforms package and prompt user
transforms_version = pkg_resources.get_distribution("rasgotransforms").version
try:
    response = requests.get('https://pypi.org/pypi/rasgotransforms/json')
    latest_version = response.json()['info']['version']
except:
    latest_version = transforms_version
if transforms_version != latest_version:
    logger.warning('You are not running the lastest version of rasgotransforms. '
                   'RasgoQL relies on this package to serve transform templates. '
                   'Please consider running `pip install rasgotransforms --upgrade` '
                   'to download our full library of SQL transforms.')

# Attempt safe imports of all DW packages so we can warn later if they are missing
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
