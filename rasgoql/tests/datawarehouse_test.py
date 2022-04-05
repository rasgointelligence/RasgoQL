import os

import pytest

from rasgoql.data import sqlalchemy, snowflake, redshift, mysql, bigquery, base

test_creds = {"username": "snek", "password": "slytherin", "host": "local", "port": 66000, "database": "super_prod",
              "schema": "very_important_schema"}


@pytest.fixture()
def base_dw():
    return base.DataWarehouse()


@pytest.fixture()
def redshift_dw():
    return redshift.DataWarehouse()


@pytest.fixture()
def redshift_creds():
    return redshift.RedshiftCredentials(**test_creds)


def test_parse_fqtn(base_dw):
    assert base_dw.parse_fqtn("db.my_scheme.table_name") == ("db", "my_scheme", "table_name")
    # assert isinstance(base_dw.parse_fqtn("db.my_scheme.table_name"), base.FQTN)

    with pytest.raises(ValueError):
        base_dw.parse_fqtn("bad value")

    with pytest.raises(ValueError):
        base_dw.parse_fqtn("bad value", strict=False)

    assert base_dw.parse_fqtn("t", default_namespace="my_db.sch", strict=False) == ("my_db", "sch", "t")
    assert base_dw.parse_fqtn("t", "my_db.sch", False) == ("my_db", "sch", "t")
    assert base_dw.parse_fqtn("schema.table", "my_db.sch", False) == ("my_db", "schema", "table")
    assert base_dw.parse_fqtn("full_db.schema.table", "my_db.sch", False) == ("full_db", "schema", "table")


def test_redshift_creds_from_env(redshift_creds):
    for key, value in test_creds.items():
        os.putenv(f"REDSHIFT_{key}", str(value))
    redshift_creds_from_env = redshift.RedshiftCredentials.from_env()

    assert redshift_creds == redshift_creds_from_env
