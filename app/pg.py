from contextlib import contextmanager
import logging
import os
from pathlib import Path
from typing import Any, Generator

import psycopg2
from pydantic import BaseModel

from app.sql import (
    copy_statement,
    create_table_statement,
    drop_table_statement,
    duration_statements,
    select_statement,
    union_statement,
)

logger = logging.getLogger(__name__)

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

COLUMN_ORDER = [
    # primary keys
    {"name": "project_id"},
    {"name": "sample"},
    # project name
    {"name": "project_name"},
    # project taxonomy
    {"name": "asset_class"},
    {"name": "project_type"},
    {"name": "project_subtype"},
    # country code
    {"name": "country_iso3"},
    # schedule columns
    {"stem_from": {"pre": "start_", "suf": "_date"}, "also": {"suf": "_year"}},
    {"stem_from": {"pre": "est_", "suf": "_date"}, "also": {"suf": "_year"}},
    {"stem_from": {"pre": "act_", "suf": "_date"}, "also": {"suf": "_year"}},
    ["_duration", ""],
]

STANDARD_DAY = "-07-02"
PRIMARY_KEYS = {"project_id", "sample"}


class Record(BaseModel):
    project_id: str
    sample: str
    data: dict[str, Any]


class DuplicateHeaderError(Exception):
    pass


class PrimaryKeysMissingError(Exception):
    pass


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
    logger.info(f"Dropping table {schema}.{table_name}...")
    cur.execute(drop_table_statement(table_name, schema))
    logger.info("Successfully dropped table.")


def create_table_from_headers(
    cur,
    table_name: str,
    headers: list[str],
    schema: str | None = None,
):
    logger.info(f"Creating table with headers: {', '.join(headers)}")
    if len(headers) != len(set(headers)):
        logger.error("Cannot create table. Duplicate headers found.")
        raise DuplicateHeaderError
    if any(pk not in headers for pk in PRIMARY_KEYS):
        logger.error("Cannot create table. Required primary keys missing.")
        raise PrimaryKeysMissingError
    _stmt = create_table_statement(table_name, headers, schema)
    logger.info(f"Executing statement: {_stmt}")
    cur.execute(_stmt)
    logger.info("Successfully created table.")


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
    with open(file_path, "r", encoding="utf-8-sig") as f, get_cursor() as cur:
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
    columns: list[str] = []
    tables_columns: dict[str, list[str]] = {}
    for table in tables:
        tables_columns[table] = []
        for col in table_columns(table):
            tables_columns[table].append(col[0])
            if col[0] not in columns:
                columns.append(col[0])
    for col in columns:
        if col.endswith("_date") and f"{col.removesuffix('_date')}_year" not in columns:
            columns.append(f"{col.removesuffix('_date')}_year")
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


def build_duration_statement(
    base_statement: str, columns: list[str]
) -> tuple[str, list[str]]:
    column_statements: list[str] = []
    visited = []
    added_columns = [
        c
        for c in [
            "act_completion_date",
            "act_completion_year",
        ]
        if c not in columns
    ]
    for column in columns:
        c = column.lower()
        if c.startswith("start_") and (c.endswith("_date") or c.endswith("_year")):
            col_stem = (
                c.removeprefix("start_").removesuffix("_year").removesuffix("_date")
            )
            if col_stem in visited:
                column_statements.append(column)
                continue
            print(f"HERE IS A DURATION COLUMN STEM: {col_stem}")
            visited.append(col_stem)
            for col in [
                f"start_{col_stem}_date",
                f"start_{col_stem}_year",
                f"est_{col_stem}_completion_date",
                f"est_{col_stem}_completion_year",
                f"est_{col_stem}_duration",
                f"act_{col_stem}_duration",
            ]:
                if col not in columns:
                    print(f"adding column: {col}")
                    added_columns.append(col)
            column_statements.append(duration_statements(col_stem))
            if not column.endswith("_duration"):
                column_statements.append(column)
        else:
            column_statements.append(column)
    if len(added_columns) > 0:
        add_statements = [f"NULL as {col}" for col in added_columns]
        base_statement = (
            f"SELECT *, {', '.join(add_statements)} FROM ({base_statement}) as y"
        )
        columns.extend(added_columns)
        column_statements.extend(
            [c for c in added_columns if not c.endswith("_duration")]
        )
    return (
        f"SELECT {', '.join(column_statements)} FROM ({base_statement}) as b",
        columns,
    )


def build_stage_statement(tables: list[str]):
    unioned_asset_class, columns = build_union_statement(tables)
    duration_statement, columns = build_duration_statement(unioned_asset_class, columns)
    print(duration_statement)
    from_statement = f"""FROM ({duration_statement}) as a
    LEFT JOIN (SELECT d1.* FROM "reference"."gdp_deflators" as d1 
    INNER JOIN (
      SELECT max(year) as year FROM "reference"."gdp_deflators") as d2 on d1.year = d2.year
    ) as d on (a.country_iso3 = d.country_code)"""

    cost_columns: list[str] = []
    idx: int = 1
    source_columns: list[str] = []
    new_column_statements: list[str] = []
    new_columns = []

    for column in columns:
        col = column.lower()
        if col.endswith("_duration") and col.startswith("act_"):
            if (c_est := f"est_{col.removeprefix('act_')}") in columns:
                col_stem = c_est.removeprefix("est_").removesuffix("_duration")
                new_column_statements.append(
                    f"{col} / {c_est} AS schedule_{col_stem}_ratio"
                )
                new_columns.append(f"schedule_{col_stem}_ratio")
        if "_cost_local_" in col:
            col_stem = (
                col.removesuffix("_year")
                .removesuffix("_currency")
                .removesuffix("_millions")
                .removesuffix("_local")
            )
            if col_stem in cost_columns:
                continue
            cost_columns.append(col_stem)
            idx += 1
            val_col = f"{col_stem}_local_millions"
            # cur_col = f"{col_stem}_local_currency"
            yr_col = f"{col_stem}_local_year"
            from_statement += f"""
            LEFT JOIN "reference"."exchange_rates" as e{idx} on (a.country_iso3 = e{idx}.country_code) and (a.{yr_col} = e{idx}.year)
            LEFT JOIN (SELECT * FROM "reference"."exchange_rates" WHERE country_code = 'USA') as f{idx} on a.{yr_col} = f{idx}.year
            LEFT JOIN "reference"."gdp_deflators" as g{idx} on (a.country_iso3 = g{idx}.country_code) and (a.{yr_col} = g{idx}.year)
            LEFT JOIN (SELECT * FROM "reference"."gdp_deflators" WHERE country_code = 'USA') as h{idx} on (a.{yr_col} = h{idx}.year)
            LEFT JOIN "reference"."ppp" as i{idx} on (a.country_iso3 = i{idx}.country_code) and (a.{yr_col} = i{idx}.year)
            LEFT JOIN (SELECT * FROM "reference"."ppp" WHERE country_code = 'USA') as j{idx} on (a.{yr_col} = j{idx}.year)
            """
            new_column_statements.append(
                f"""a.{val_col} * f{idx}.exchange_rate * d.deflation_factor 
            / e{idx}.exchange_rate  / g{idx}.deflation_factor as {col_stem}_norm_millions"""
            )
            new_column_statements.append(
                f"""a.{val_col} * e{idx}.exchange_rate * j{idx}.exchange_rate * d.deflation_factor
                / f{idx}.exchange_rate / i{idx}.exchange_rate / h{idx}.deflation_factor as {col_stem}_norm_ppp_millions
                """
            )
            new_column_statements.append(f"'USD' as {col_stem}_norm_currency")
            new_column_statements.append(f"d.year as {col_stem}_norm_year")
            new_columns.append(f"{col_stem}_norm_millions")
            new_columns.append(f"{col_stem}_norm_ppp_millions")
            new_columns.append(f"{col_stem}_norm_currency")
            new_columns.append(f"{col_stem}_norm_year")
        if "source" in col:
            source_columns.append(column)
    if len(source_columns) > 0:
        new_columns.append("citations")
        new_column_statements.append(
            f"ARRAY_REMOVE(ARRAY[{', '.join(source_columns)}], null) AS citations"
        )
    columns = [c for c in columns if c not in source_columns]
    column_statements = set(columns + new_column_statements)
    columns += new_columns
    stmt = f"SELECT {', '.join(column_statements)} {from_statement}"
    additional_statements = []
    additional_columns = []
    for col in columns:
        if all(
            [
                "_cost_norm_" in col,
                col.startswith("est_"),
                col.endswith("_millions"),
            ]
        ):
            if (act_col := f"act_{col.removeprefix('est_')}") in columns:
                rat_col = f"{col.removeprefix('est_').removesuffix('_millions')}_ratio"
                additional_statements.append(f"{act_col} / {col} as {rat_col}")
                additional_columns.append(rat_col)
    new_statement = (
        f"SELECT {', '.join(columns + additional_statements)} FROM ({stmt}) as z"
    )
    columns += additional_columns
    return new_statement


def enforce_column_order(column_statements: list[str] | set[str]) -> list[str]:
    output_column_statements: list[str] = []
    pass
