import csv
from datetime import date
import logging
from pathlib import Path
from typing import Optional

from sqlmodel import Field

from app.sql import raw_schema
from app.table import DatabaseManager


COLUMN_TYPE_NOTATION = {
    int: {"suffixes": ["_year"], "prefixes": []},
    bool: {"suffixes": [], "prefixes": ["is_"]},
    date: {"suffixes": ["_date"], "prefixes": []},
    float: {
        "suffixes": ["_millions", "_value", "_ratio", "_duration", "_thousands"],
        "prefixes": [],
    },
    str: {"suffixes": [""], "prefixes": []},
}
STANDARD_DAY = "-07-02"
PRIMARY_KEYS = {"project_id", "sample"}

_logger = logging.getLogger(__name__)


def data_type(header: str) -> type:
    lower_header = header.lower()
    for dtype, conditions in COLUMN_TYPE_NOTATION.items():
        if any(
            lower_header.endswith(suffix) for suffix in conditions["suffixes"]
        ) or any(lower_header.endswith(prefix) for prefix in conditions["prefixes"]):
            return dtype


def column_details(headers: list[str]) -> dict[str, tuple[type, Field]]:
    details = {}
    for header in headers:
        pk = header in PRIMARY_KEYS
        db_type = data_type(header)
        details[header] = (
            db_type if pk else Optional[db_type],
            Field(default="" if pk else None, primary_key=pk)
        )
    return details


def load_raw_data(file_path: Path, db: DatabaseManager):
    table_name = file_path.stem
    schema = f"raw_{file_path.parent.parent.stem}"
    if db.tables.get(schema, {}).get(table_name) is None:
        _logger.info(f"Adding existing table: {schema}.{table_name} to DB manager.")
        db.map_existing_table(table_name, schema)
    if db.table_exists(table_name, schema):
        db.drop_table(table_name, schema)
    with open(file_path, "r", encoding="utf-8-sig", newline='') as f:
        data = csv.DictReader(f)
        headers = [k for k in data[0].keys()]
        col_desc = column_details(headers)
        db.create_new_table(table_name, schema, col_desc)
        with db.get_session() as session:
            for row in data:
                session.add(db.tables[schema][table_name](**row))
            session.commit()
            
    return f"{schema}.{table_name}"


def drop_raw_table(table_name: str, verified: bool, db: DatabaseManager):
    db.drop_table(table_name, raw_schema(verified))
