import os

from google.cloud.sql.connector import Connector
from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, text

EXCHANGE_RATE_URI = "PA.NUS.FCRF"
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")

connector = Connector()


def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )
    return conn


def select_data(table_name: str, schema_name: str = "raw") -> DataFrame:
    pool = create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    with pool.connect() as connection:
        a = connection.execute(
            text(f'SELECT * FROM "{schema_name}"."{table_name}"')
        )
    cols = [i for i in a.keys()]
    return [{c: r for r, c in zip(row, cols)} for row in a.all()]


def union_prod():
    pool = create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    m = MetaData(schema="stage")
    m.reflect(pool)
    return [table for table in m.tables.values()]
