from csv import DictReader, DictWriter
from datetime import date, datetime
import logging
from pathlib import Path
from tqdm import tqdm
from typing import Any, Optional

from sqlmodel import (
    and_,
    case,
    cast,
    delete,
    Field,
    select,
    Integer,
    Table,
    Float,
    text,
    null,
    true,
    union,
    extract,
    func,
    Date,
    join,
    literal,
    literal_column,
)
import wbgapi as wb

from app.filesys import get_data_files
from app.sql import raw_schema, stage_schema
from app.table import CreateTableAs, DatabaseManager

EXCHANGE_RATE_TABLE = "PA.NUS.FCRF"
PPP_RATE_TABLE = "PA.NUS.PPP"
COLUMN_TYPE_NOTATION = {
    int: {"suffixes": ["year"], "prefixes": []},
    bool: {"suffixes": [], "prefixes": ["is_"]},
    date: {"suffixes": ["_date"], "prefixes": []},
    float: {
        "suffixes": [
            "_millions",
            "_value",
            "_ratio",
            "_duration",
            "_thousands",
            "_rate",
        ],
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
    primary_keys: list[str] = PRIMARY_KEYS,
) -> tuple[dict[str, tuple[type, Field]], dict[str, type]]:
    details = {}
    for header in headers:
        pk = header in primary_keys
        dtype = data_type(header)
        details[header] = (
            dtype if pk else Optional[dtype],
            Field(default="" if pk else None, primary_key=pk),
        )
    return details


def load_exchange_rate(db: DatabaseManager):
    if db.table_exists("exchange_rates", "reference"):
        if db.tables.get("reference", {}).get("exchange_rates") is None:
            db.map_existing_table("exchange_rates", "reference")
    with db.get_session() as session:
        session.exec(
            delete(db.tables["reference"]["exchange_rates"])
        )
        session.commit()

    # col_desc = column_details(
    #     ["country_iso3", "year", "exchange_rate"], ["country_iso3", "year"]
    # )
    with db.get_session() as session:
        session.bulk_insert_mappings(
            db.tables["reference"]["exchange_rates"],
            [
                {
                    "country_iso3": d["economy"],
                    "year": int(d["time"][2:]),
                    "exchange_rate": d["value"],
                }
                for d in wb.data.fetch("PA.NUS.FCRF")
                if d["value"] is not None
            ],
        )
        session.commit()


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


def date_year_statement(table):
    new_table = add_schedule_columns(table)
    cases = []
    for c in new_table.selected_columns:
        if c.name.endswith("_date"):
            cases.extend(
                date_year_case_statements(new_table, c.name.removesuffix("_date"))
            )
    case_names = [case.name for case in cases]
    return select(
        *[c for c in new_table.selected_columns if c.name not in case_names],
        *cases,
    )


def date_year_case_statements(table: Table, column_stem: str):
    year_column_name = f"{column_stem}_year"
    date_column_name = f"{column_stem}_date"
    year_column = table.selected_columns.get(year_column_name)
    if year_column is None:
        return []
    date_column = table.selected_columns.get(date_column_name)
    return [
        case(
            (year_column.is_(None), cast(extract("year", date_column), Integer)),
            else_=year_column,
        ).label(year_column_name),
        case(
            (
                and_(date_column.is_(None), year_column.isnot(None)),
                cast(func.concat(year_column, literal(STANDARD_DAY)), Date),
            ),
            else_=date_column,
        ).label(date_column_name),
    ]


def duration_statement(table):
    cases = []
    for c in table.selected_columns:
        if c.name.endswith("_duration"):
            cases.append(
                duration_case_statement(table, c.name.removesuffix("_duration"))
            )
    case_names = [case.name for case in cases]
    return select(
        *[c for c in table.selected_columns if c.name not in case_names],
        *cases,
    )


def duration_case_statement(table, column_stem: str):
    duration_id = column_stem.removeprefix("est_").removeprefix("act_")
    start_year = table.selected_columns.get(f"start_{duration_id}_year")
    start_date = table.selected_columns.get(f"start_{duration_id}_date")
    end_date = table.selected_columns.get(f"{column_stem}_completion_date")
    end_year = table.selected_columns.get(f"{column_stem}_completion_year")
    duration = table.selected_columns.get(f"{column_stem}_duration")
    if any(c is None for c in [start_year, start_date, end_date, end_year]):
        return duration
    return case(
        (
            duration.is_(None),
            cast(
                case(
                    (
                        and_(end_date.is_(None), end_year.isnot(None)),
                        cast(func.concat(end_year, literal(STANDARD_DAY)), Date),
                    ),
                    else_=end_date,
                )
                - case(
                    (
                        and_(start_date.is_(None), start_year.isnot(None)),
                        cast(func.concat(start_year, literal(STANDARD_DAY)), Date),
                    ),
                    else_=start_date,
                ),
                Float,
            )
            / 365,
        ),
        else_=duration,
    ).label(duration.name)


def add_schedule_columns(table):
    all_columns = [c.name for c in table.columns]
    new_columns = []
    for c in table.selected_columns:
        if c.name.startswith("start_") and c.name.endswith("_date"):
            col_stem = c.name.removesuffix("_date").removeprefix("start_")
            if col_stem.endswith("_completion"):
                col_stem = col_stem.removesuffix("_completion")
            for col, dtype in [
                (f"start_{col_stem}_date", Date),
                (f"start_{col_stem}_year", Integer),
                (f"est_{col_stem}_completion_date", Date),
                (f"est_{col_stem}_completion_year", Integer),
                (f"est_{col_stem}_duration", Float),
                (f"act_{col_stem}_duration", Float),
            ]:
                if col not in all_columns:
                    new_columns.append(cast(null(), dtype).label(col))
    return select(table.subquery(), *new_columns)


def convert_costs(table, db):
    cost_columns = []
    for c in table.selected_columns:
        if "_cost_local_" in c.name:
            col_stem = (
                c.name.removesuffix("_year")
                .removesuffix("_currency")
                .removesuffix("_millions")
                .removesuffix("_local")
            )
            if col_stem in cost_columns:
                continue
            val_col = f"{col_stem}_local_millions"
            yr_col = f"{col_stem}_local_year"
            join(table.subquery(), db.tables["reference"]["exchange_rates"])


def schedule_ratio(table):
    new_columns = []
    for c in table.selected_columns:
        if (
            c.name.startswith("act_")
            and c.name.endswith("_duration")
            and f"est_{c.name.removeprefix('act_')}" in table.selected_columns
        ):
            est_col = table.selected_columns.get(f"est_{c.name.removeprefix('act_')}")
            new_columns.append(
                (c / est_col).label(
                    f"schedule_{c.name.removeprefix('act_').removesuffix('duration')}ratio"
                )
            )
    return select(
        *[col for col in table.selected_columns if col.name not in new_columns],
        *new_columns,
    )


def stage_data(asset_class: str, db: DatabaseManager, verified: bool = True):
    # get all raw tables of the asset class
    tables = [f.stem for f in get_data_files(asset_class, verified)]
    for table in tables:
        db.map_existing_table(table, raw_schema(verified))
    schema = stage_schema(verified)
    if db.table_exists(asset_class, schema):
        db.drop_table(asset_class, schema)
    union_stmt = union(
        *[select(db.tables[raw_schema(verified)][table]) for table in tables]
    )
    date_statement = date_year_statement(union_stmt)
    dur_statement = duration_statement(date_statement).subquery()
    db.map_existing_table("gdp_deflators", "reference")
    deflator_table = db.tables["reference"]["gdp_deflators"]
    max_year = select(func.max(deflator_table.year).label("year")).scalar_subquery()
    latest_year_deflators = (
        select(deflator_table).filter(deflator_table.year == max_year).subquery()
    )
    with_deflators = join(
        dur_statement,
        latest_year_deflators,
        dur_statement.c.country_iso3 == latest_year_deflators.c.country_code,
    ).select()

    with db.get_session() as session:
        session.exec(CreateTableAs(f"{schema}.{asset_class}", union_stmt))
        session.commit()
        session.exec(
            text(
                f"""ALTER TABLE "{schema}"."port" ADD PRIMARY KEY (project_id, sample);"""
            )
        )
