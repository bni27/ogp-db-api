import os

from google.cloud.sql.connector import Connector
from pandas import DataFrame
import sqlalchemy

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
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    with pool.connect() as connection:
        a = connection.execute(
            sqlalchemy.text(f'SELECT * FROM "{schema_name}"."{table_name}"')
        )
    return DataFrame(a.all(), columns=a.keys()).to_dict()
