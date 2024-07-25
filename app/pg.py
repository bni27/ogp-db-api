from contextlib import contextmanager
import os
from pathlib import Path
from typing import Any, Generator, Iterable

import psycopg2

from app.sql import (
    copy_statement,
    create_table_statement,
    drop_table_statement,
    select_statement,
    union_statement,
)


DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")

POSTGRES_TYPES = {
    23: "INTEGER",
    1043: "VARCHAR",
    701: "FLOAT",
    1082: "DATE",
    16: "BOOLEAN",
}


@contextmanager
def get_cursor():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        host=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        user=DB_USER,
        password=DB_PASS,
    )
    yield conn.cursor()
    conn.commit()
    conn.close()
    return


def drop_table(cur, table_name: str, schema: str = "raw"):
    cur.execute(drop_table_statement(table_name, schema))


def create_table_from_headers(
    cur,
    table_name: str,
    headers: list[str],
    schema: str | None = None,
):
    cur.execute(create_table_statement(table_name, headers, schema))


def create_table_from_select(
    cur,
    table_name: str,
    select_statement: str,
    schema: str | None = None,
):
    table_str = table_name if schema is None else f"{schema}.{table_name}"
    cur.execute(
       f"CREATE TABLE {table_str} AS {select_statement}"
    )


def load_data_from_file(file_path: Path, table_name: str):
    schema = f"raw_{file_path.parent.parent.stem}"
    with open(file_path, "r") as f, get_cursor() as cur:
        drop_table(cur, table_name, schema)
        header_line = f.readline()
        headers = header_line.split(",")
        create_table_from_headers(cur, table_name, headers, schema)
        cur.copy_expert(copy_statement(table_name, header_line, schema), f)
    return f"{schema}.{table_name}"


def select_data(
    table_name: str,
    schema: str,
    columns: list[str] | str = "*",
    where: str | None = None,
    limit: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    statement = select_statement(table_name, schema, columns, where, limit)
    with get_cursor() as cur:
        cur.execute(statement)
        rows = cur.fetchall()
        cols = cur.description
        return ({c: row[i] for i, c in enumerate(cols)} for row in rows)


def row_count(table_name: str, schema: str | None = None):
    table_str = table_name if schema is None else f"{schema}.{table_name}"
    with get_cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_str};")
        return cur.fetchone()


def all_tables_in_schema(schema: str) -> Generator[str, None, None]:
    where = f"WHERE table_schema = '{schema}'"
    q_results = select_statement(
        "tables",
        "information_schema",
        ["table_name"],
        where,
    )
    return (r["table_name"] for r in q_results)


def table_columns(table_name: str, schema: str | None = None) -> Generator[tuple[str, str], None, None]:
    statement = select_statement(table_name, "*", schema, limit=0)
    with get_cursor() as cur:
        cur.execute(statement)
        return ((c.name, POSTGRES_TYPES.get(c.type_code, "VARCHAR")) for c in cur.description)


def union_all_in_schema(schema: str, target_table: str, target_schema: str):
    tables = (f"{schema}.{t}" for t in all_tables_in_schema(schema))
    union_tables(tables, target_table, target_schema)


def union_tables(tables: Iterable[str], target_table: str, target_schema: str):
    columns = []
    tables_columns = {}
    for table in tables:
        tables_columns[table] = []
        for col in table_columns(table):
            if col not in columns:
                columns.append(col)
                tables_columns[table].append(col)
    union_headers = {}
    for table in tables:
        union_headers[table] = [
            (col if col in tables_columns[table] else f"NULL as {col}") 
            for col in columns
        ]
    union_selects = union_statement(*(select_statement(table, columns=union_headers[table]) for table in tables))    
    with get_cursor() as cur:
        drop_table(cur, target_table, target_schema)
        create_table_from_select(cur, target_table, union_selects, target_schema)
