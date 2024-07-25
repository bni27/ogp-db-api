import os
from pathlib import Path

from app.filesys import get_data_files
from app.pg import (
    load_data_from_file,
    union_all_in_schema,
    union_tables,
)
from app.sql import raw_schema, stage_schema, _verified_name

DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_IP = os.environ.get("DB_IP")


def load_raw_data(file_path: Path):
    return load_data_from_file(file_path, file_path.stem)


def stage_data(asset_class: str, verified: bool = False):
    tables = [f"{raw_schema(verified)}.{f.stem}" for f in get_data_files(asset_class, verified)]
    union_tables(tables, asset_class, stage_schema(verified))


def union_prod(verified: bool = False):
    union_all_in_schema(stage_schema(verified), _verified_name(verified) , "prod")