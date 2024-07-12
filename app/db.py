import os

from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, MetaData, text

DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")


def connector():
    conn = Connector()
    pool = create_engine(
        "postgresql+pg8000://",
        creator=conn.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
        ),
    )
    return pool


def select_data(table_name: str, schema_name: str = "prod") -> list[dict]:
    with connector().connect() as connection:
        a = connection.execute(
            text(f'SELECT * FROM "{schema_name}"."{table_name}"')
        )
    cols = [i for i in a.keys()]
    return [{c: r for r, c in zip(row, cols)} for row in a.all()]


def union_prod():
    m = MetaData(schema="stage")
    m.reflect(connector())
    columns = []
    for table in m.tables.values():
        for column in table.c:
            if column not in columns:
                columns.append(column.name)

    return columns
