import os
from pathlib import Path

from app.filesys import get_data_files
from app.pg import (
    build_stage_statement,
    create_table_from_select,
    drop_table,
    get_cursor,
    load_data_from_file,
    union_all_in_schema,
)
from app.sql import prod_table, raw_schema, stage_schema

DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_IP = os.environ.get("DB_IP")


def load_raw_data(file_path: Path):
    return load_data_from_file(file_path, file_path.stem)


def stage_data(asset_class: str, verified: bool = True):
    # get all raw tables of the asset class
    tables = [
        f"{raw_schema(verified)}.{f.stem}"
        for f in get_data_files(asset_class, verified)
    ]
    with get_cursor() as cur:
        schema = stage_schema(verified)
        drop_table(cur, asset_class, schema)
        create_table_from_select(
            cur, asset_class, build_stage_statement(tables), schema
        )


def delete_raw_table(file_name: str, verified: bool = True):
    with get_cursor() as cur:
        drop_table(cur, file_name, raw_schema(verified))


def delete_stage_table(asset_class: str, verified: bool = True):
    with get_cursor() as cur:
        drop_table(cur, asset_class, stage_schema(verified))


def union_prod(verified: bool = True):
    union_all_in_schema(stage_schema(verified), prod_table(verified), "prod")


def update_reference(table: str) -> None:
    pass
