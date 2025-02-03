from csv import DictReader, DictWriter
import logging
from os import environ
from pathlib import Path
from typing import Any, Generator

from app.db import DatabaseManager

BUCKET_MOUNT = Path(environ.get("BUCKET_MOUNT", "/data"))
_logger = logging.getLogger(__name__)


def build_verified_path(verified: bool = False) -> Path:
    verification_folder = Path("verified") if verified else Path("unverified")
    return BUCKET_MOUNT / verification_folder


def build_asset_path(
    asset_class: str,
    verified: bool = False,
    create: bool = False,
    raise_if_absent: bool = True,
) -> Path:
    asset_class_path = build_verified_path(verified) / Path(asset_class)
    if create:
        asset_class_path.mkdir()
    if raise_if_absent and not asset_class_path.exists():
        raise ValueError
    return asset_class_path


def build_raw_file_path(
    file_name: str,
    asset_class: str,
    verified: bool = False,
) -> Path:
    return build_asset_path(asset_class, verified) / Path(file_name)


def _get_files(
    asset_class: str,
    verified: bool = False,
) -> Generator[Path, None, None]:
    return (f for f in build_asset_path(asset_class, verified).iterdir() if f.is_file())


def get_data_files(
    asset_class: str, verified: bool = False, extension: str = ".csv"
) -> Generator[Path, None, None]:
    return (f for f in _get_files(asset_class, verified) if f.name.endswith(extension))


def get_directories(
    verified: bool = False,
) -> Generator[Path, None, None]:
    return (d for d in build_verified_path(verified).iterdir() if d.is_dir())


def find_file(table_name: str, verified: bool = False) -> Path:
    for d in get_directories(verified):
        for f in get_data_files(d, verified):
            if f.stem == table_name:
                return build_raw_file_path(f, d, verified)


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