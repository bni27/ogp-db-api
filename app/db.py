import os

from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, MetaData, text

DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")

connector = Connector()


def get_conn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )
    return conn


def get_pool():
    return create_engine(
        "postgresql+pg8000://",
        creator=get_conn,
    )


def select_data(table_name: str, schema_name: str = "prod") -> list[dict]:
    with get_pool().connect() as connection:
        a = connection.execute(text(f'SELECT * FROM "{schema_name}"."{table_name}"'))
    cols = [i for i in a.keys()]
    return [{c: r for r, c in zip(row, cols)} for row in a.all()]


def union_prod():
    m = MetaData(schema="stage")
    m.reflect(get_pool())
    columns = []
    for table in m.tables.values():
        for column in table.c:
            if column not in columns:
                columns.append(column.name)
    return columns
