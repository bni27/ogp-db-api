from contextlib import contextmanager
import os
from pathlib import Path
from typing import Any, Generator

import psycopg2

from app.sql import (
    case_if_year_and_date,
    copy_statement,
    create_table_statement,
    drop_table_statement,
    duration_statements,
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

STANDARD_DAY = "-07-02"


STAGE_JOINS: list[tuple[str, str]] = [
    ('"reference"."exchange_rates', ""),
    ('"reference"."gdp_deflators"', ""),
]


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
    cur.execute(f"CREATE TABLE {table_str} AS {select_statement}")


def load_data_from_file(file_path: Path, table_name: str):
    schema = f"raw_{file_path.parent.parent.stem}"
    with open(file_path, "r", encoding='utf-8-sig') as f, get_cursor() as cur:
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
    statement = select_statement(table_name, columns, schema, where, limit)
    return _select(statement)


def _select(statement: str) -> Generator[dict[str, Any], None, None]:
    with get_cursor() as cur:
        cur.execute(statement)
        rows = cur.fetchall()
        cols = cur.description
        return [{c.name: row[i] for i, c in enumerate(cols)} for row in rows]


def row_count(table_name: str, schema: str | None = None):
    table_str = table_name if schema is None else f"{schema}.{table_name}"
    with get_cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_str};")
        return cur.fetchone()


def all_tables_in_schema(schema: str) -> Generator[str, None, None]:
    where = f"WHERE table_schema = '{schema}'"
    statement = select_statement(
        "tables",
        ["table_name"],
        "information_schema",
        where,
    )
    return (r["table_name"] for r in _select(statement))


def table_columns(
    table_name: str, schema: str | None = None
) -> Generator[tuple[str, str], None, None]:
    statement = select_statement(table_name, "*", schema, limit=0)
    with get_cursor() as cur:
        cur.execute(statement)
        return (
            (c.name, POSTGRES_TYPES.get(c.type_code, "VARCHAR"))
            for c in cur.description
        )


def union_all_in_schema(schema: str, target_table: str, target_schema: str):
    tables = [f"{schema}.{t}" for t in all_tables_in_schema(schema)]
    union_tables(tables, target_table, target_schema)


def union_tables(tables: list[str], target_table: str, target_schema: str):
    union_selects, _ = build_union_statement(tables)
    with get_cursor() as cur:
        drop_table(cur, target_table, target_schema)
        create_table_from_select(cur, target_table, union_selects, target_schema)


def build_union_statement(tables: list[str]) -> tuple[str, list[str]]:
    columns = []
    tables_columns = {}
    for table in tables:
        print(table)
        tables_columns[table] = []
        for col in table_columns(table):
            tables_columns[table].append(col[0])
            if col[0] not in columns:
                columns.append(col[0])
    union_headers = {
        table: [
            col if col in tables_columns[table] else f"NULL as {col}" for col in columns
        ]
        for table in tables
    }
    return (
        union_statement(
            *(select_statement(t, columns=union_headers[t]) for t in tables)
        ),
        columns,
    )


def build_year_date_statement(
    base_statement: str, columns: list[str]
) -> tuple[str, list[str]]:
    column_statements = []
    return_columns = []
    for col in columns:
        c = col.lower()
        col_stem = c.removesuffix("_year")
        return_columns.append(c)
        if c.endswith("_year") and f"{col_stem}_date" in columns:
            column_statements.append(case_if_year_and_date(col_stem))
        else:
            column_statements.append(col.lower())
        if col_stem.startswith("start_"):
            col_middle = col_stem.removeprefix("start_")
            if (n := f"act_{col_middle}_duration") not in columns:
                column_statements.append(f"NULL as {n}")
                return_columns.append(n)
            if (n := f"est_{col_middle}_duration") not in columns:
                column_statements.append(f"NULL as {n}")
                return_columns.append(n)

    return (
        f"SELECT {', '.join(column_statements)} FROM ({base_statement})",
        return_columns,
    )


def build_duration_statement(base_statement: str, columns: list[str]) -> tuple[str, list[str]]:
    column_statements = []
    visited = []
    added_columns = [
        c
        for c in [
            "est_completion_date",
            "est_completion_year",
            "act_completion_date",
            "act_completion_year",
        ]
        if c not in columns
    ]
    for column in columns:
        c = column.lower()
        if c.startswith("start_") and (col_stem := c.removeprefix("start_").removesuffix("_year").removesuffix("_date")) not in visited:
            visited.append(col_stem)
            for col in [
                    f"start_{col_stem}_date",
                    f"start_{col_stem}_year",
                ]:
                if col not in columns:
                    added_columns.append(col)
            column_statements.append(duration_statements(col_stem))
        else:
            column_statements.append(column)
    
    if len(added_columns) > 0:
        add_statements = [f"NULL as {col}" for col in added_columns]
        base_statement = f"SELECT *, {', '.join(add_statements)} FROM ({base_statement}) as y"
        columns.extend(added_columns)
        column_statements.extend(added_columns)
    return (
        f"SELECT {', '.join(column_statements)} FROM ({base_statement}) as b",
        columns,
    )


def build_stage_statement(tables: list[str]):
    unioned_asset_class, columns = build_union_statement(tables)
    duration_statement, columns2 = build_duration_statement(unioned_asset_class, columns)
    from_statement = f"""FROM ({duration_statement}) as a
    LEFT JOIN (SELECT d1.* FROM "reference"."gdp_deflators" as d1 INNER JOIN (SELECT max(year) as year FROM "reference"."gdp_deflators") as d2 on d1.year = d2.year) as h on (a.country_iso3 = h.country_code)"""

    cost_columns: list[tuple[str, int]] = []
    idx = 1
    new_column_statements = []
    new_columns = []
    for column in columns2:
        if "_cost_local_" in column.lower():
            col_stem = column.lower().removesuffix("_year").removesuffix("_currency").removesuffix("_millions").removesuffix("_local")
            if col_stem in cost_columns:
                continue
            cost_columns.append((col_stem, idx))
            idx += 1
            val_col = f"{col_stem}_local_millions"
            # cur_col = f"{col_stem}_local_currency"
            yr_col = f"{col_stem}_local_year"
            from_statement += f"""
            LEFT JOIN "reference"."exchange_rates" as e{idx} on (a.country_iso3 = e{idx}.country_code) and (a.{yr_col} = e{idx}.year)
            LEFT JOIN (SELECT * FROM "reference"."exchange_rates" WHERE country_code = 'USA') as f{idx} on a.{yr_col} = f{idx}.year
            LEFT JOIN "reference"."gdp_deflators" as g{idx} on (a.country_iso3 = g{idx}.country_code) and (a.{yr_col} = g{idx}.year)"""
            new_column_statements.append(f"""a.{val_col} * f{idx}.exchange_rate * h.deflation_factor 
            / e{idx}.exchange_rate  / g{idx}.deflation_factor as {col_stem}_norm_millions""")
            new_column_statements.append(f"'USD' as {col_stem}_norm_currency")
            new_column_statements.append(f"h.year as {col_stem}_norm_year")
            new_columns.append(f"{col_stem}_norm_millions")
            new_columns.append(f"{col_stem}_norm_currency")
            new_columns.append(f"{col_stem}_norm_year")
    column_statements = columns2 + new_column_statements
    column_statements = set(column_statements)
    stmt = f"SELECT {', '.join(column_statements)} {from_statement}"
    print(stmt.replace("\n", " "))
    return stmt