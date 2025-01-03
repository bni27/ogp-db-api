from csv import DictReader, DictWriter
from datetime import date, datetime
import logging
from pathlib import Path
from typing import Any, Optional

from sqlmodel import Field, select, union

from app.filesys import get_data_files
from app.sql import raw_schema, stage_schema
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
DATE_FORMAT = "%Y-%m-%d"
PRIMARY_KEYS = {"project_id", "sample"}

_logger = logging.getLogger(__name__)


def type_cast(column: str, value: str) -> int | bool | date | float | str:
    dtype = data_type(column)
    if dtype in [int, float]:
        return dtype(value)
    if dtype == bool:
        return value.lower() in ["y", "yes", "t", "true", "on", "1"]
    if dtype == date:
        return datetime.strptime(value, DATE_FORMAT).date()
    return value


def data_type(header: str) -> type:
    lower_header = header.lower()
    for dtype, conditions in COLUMN_TYPE_NOTATION.items():
        if any(
            lower_header.endswith(suffix) for suffix in conditions["suffixes"]
        ) or any(lower_header.startswith(prefix) for prefix in conditions["prefixes"]):
            return dtype


def column_details(
    headers: list[str],
) -> tuple[dict[str, tuple[type, Field]], dict[str, type]]:
    details = {}
    for header in headers:
        pk = header in PRIMARY_KEYS
        dtype = data_type(header)
        details[header] = (
            dtype if pk else Optional[dtype],
            Field(default="" if pk else None, primary_key=pk),
        )
    return details


def load_raw_data(file_path: Path, db: DatabaseManager, verified: bool = True):
    table_name = file_path.stem
    schema = raw_schema(verified)
    if db.table_exists(table_name, schema):
        if db.tables.get(schema, {}).get(table_name) is None:
            _logger.info(f"Adding existing table: {schema}.{table_name} to DB manager.")
            db.map_existing_table(table_name, schema)
        db.drop_table(table_name, schema)
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        data = DictReader(f)
        first_row = next(data)
        headers = [k for k in first_row.keys()]
        col_desc = column_details(headers)
        db.create_new_table(table_name, schema, col_desc)
        with db.get_session() as session:
            session.add(
                db.tables[schema][table_name](
                    **{k: type_cast(k, v) for k, v in first_row.items() if v != ""}
                )
            )
            for row in data:
                session.add(
                    db.tables[schema][table_name](
                        **{k: type_cast(k, v) for k, v in row.items() if v != ""}
                    )
                )
            session.commit()
    return f"{schema}.{table_name}"


def drop_raw_table(table_name: str, verified: bool, db: DatabaseManager):
    db.drop_table(table_name, raw_schema(verified))


def delete_record_from_file(file_path: Path, project_id: str, sample: str):
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        data = [r for r in DictReader(f)]
    fieldnames = data[0].keys()
    if (
        len(
            [r for r in data if r["project_id"] == project_id and r["sample"] == sample]
        )
        == 0
    ):
        raise ValueError("Record does not exist.")
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        w = DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in data:
            if r["project_id"] == project_id and r["sample"] == sample:
                continue
            w.writerow(r)


def update_record_in_file(
    file_path: Path, project_id: str, sample: str, data: dict[str, Any]
):
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        rows = [r for r in DictReader(f)]
    fieldnames = rows[0].keys()
    if (
        len(
            [r for r in rows if r["project_id"] == project_id and r["sample"] == sample]
        )
        == 0
    ):
        raise ValueError("Record does not exist.")
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        w = DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            if r["project_id"] == project_id and r["sample"] == sample:
                r.update(data)
            w.writerow(r)


def add_record_in_file(
    file_path: Path, project_id: str, sample: str, data: dict[str, Any]
):
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        rows = [r for r in DictReader(f)]
    fieldnames = rows[0].keys()
    if (
        len(
            [r for r in rows if r["project_id"] == project_id and r["sample"] == sample]
        )
        > 0
    ):
        raise ValueError("Record already exists.")
    with open(file_path, "a", encoding="utf-8-sig", newline="") as f:
        w = DictWriter(f, fieldnames=fieldnames)
        w.writerow({**data, "project_id": project_id, "sample": sample})


def stage_data(asset_class: str,  db: DatabaseManager, verified: bool = True):
    # get all raw tables of the asset class
    tables = [f.stem for f in get_data_files(asset_class, verified)]
    for table in tables:
        db.map_existing_table(table, raw_schema(verified))
    schema = stage_schema(verified)
    if db.table_exists(asset_class, schema):
        db.drop_table(asset_class, schema)
    union_stmt = union(*[select(db.tables[raw_schema(verified)][table]) for table in tables])
    with db.get_session() as session:
        data = session.exec(union_stmt)
        return data.fetch_all()