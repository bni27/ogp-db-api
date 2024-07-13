import os
from pathlib import Path

from google.cloud.sql.connector import Connector
import psycopg2
from sqlalchemy import create_engine, MetaData, text


DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_IP = os.environ.get("DB_IP")

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


def load_raw(file_path: Path):
    schema = "raw"
    table_name = file_path.name.split(".")[0]
    drop_table(table_name, schema)
    load_data_into_table(file_path)


def drop_table(table_name: str, schema: str):
    with get_pool().connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
        connection.commit()


def create_table_from_headers(headers: list[str], table_name: str, schema: str = "raw"):
    with get_pool().connect() as connection:
        col_str = gen_col_str(headers, ["project_id", "sample"])
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} 
                ( {col_str}
                )
                """
            )
        )
        connection.commit()


def load_data_into_table(file_path: Path):
    table_name = file_path.name.split(".")[0]
    with open(file_path, "r") as f:
        header_line = f.readline()
        headers = header_line.split(",")
        create_table_from_headers(headers, table_name)
        conn = psycopg2.connect(
            dbname=DB_NAME, host=DB_IP, port=5432, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        cmd = f"COPY {table_name}({headers}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
        cursor.copy_expert(cmd, f)
        conn.commit()


def gen_col_str(
    columns: list[str],
    primary_key: str | list[str] | None = None,
    default_type: str = "varchar",
    special_type: dict[str, str] | None = None,
) -> str:
    column_strings = []
    if special_type is None:
        special_type = {}
    column_types = {}
    for k, v in special_type.items():
        if isinstance(v, str):
            column_types[v] = k
        else:
            column_types.update({i: k for i in v})
    if primary_key is None:
        primary_key = [columns[0]]
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    for col in columns:
        column_strings.append(col.lower())
        if col.lower() in column_types:
            column_strings[-1] += " " + column_types.get(col.lower(), "varchar")
        else:
            column_strings[-1] += " " + default_type
        if col.lower() in primary_key:
            column_strings[-1] += " NOT NULL"
    if primary_key is None:
        column_strings.append(f"PRIMARY KEY {columns[0]}")
    elif isinstance(primary_key, str):
        column_strings.append(f"PRIMARY KEY {primary_key}")
    elif isinstance(primary_key, list):
        column_strings.append(f"PRIMARY KEY ({', '.join(primary_key)})")
    return ", ".join(column_strings)


def union_prod():
    m = MetaData(schema="stage")
    m.reflect(get_pool())
    columns = []
    for table in m.tables.values():
        for column in table.c:
            if column not in columns:
                columns.append(column.name)
    return columns
