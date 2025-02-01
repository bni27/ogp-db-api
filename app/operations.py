from csv import DictReader, DictWriter
from datetime import date, datetime
import logging
from pathlib import Path
from typing import Any, Optional

import requests
from sqlmodel import (
    and_,
    case,
    cast,
    Field,
    select,
    Integer,
    Table,
    Float,
    text,
    null,
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
from app.sql import prod_table, raw_schema, stage_schema
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


def load_gdp_deflators(db: DatabaseManager):
    if db.table_exists("gdp_deflators", "reference"):
        db.truncate_table("gdp_deflators", "reference")
    else:
        col_desc = column_details(
            ["country_iso3", "year", "gdp_deflator"], ["country_iso3", "year"]
        )
        db.create_new_table("gdp_deflators", "reference", col_desc)
    data = []
    for i in range(1, 3):
        resp = requests.get(
            f"https://api.worldbank.org/v2/en/sources/2/series/NY.GDP.DEFL.ZS/country/all/time/all?per_page=10000&page={i}&format=json"
        )
        data.extend(
            [
                {"value": r["value"], **{v["concept"]: v["id"] for v in r["variable"]}}
                for r in resp.json()["source"]["data"]
                if r["value"] is not None
            ]
        )
    with db.get_session() as session:
        session.bulk_insert_mappings(
            db.tables["reference"]["gdp_deflators"],
            [
                {
                    "country_iso3": d["Country"],
                    "year": d["Time"][2:],
                    "gdp_deflator": d["value"],
                }
                for d in data
            ],
        )
        session.commit()


def load_exchange_rate(db: DatabaseManager):
    if db.table_exists("exchange_rates", "reference"):
        db.truncate_table("exchange_rates", "reference")
    else:
        col_desc = column_details(
            ["country_iso3", "year", "exchange_rate"], ["country_iso3", "year"]
        )
        db.create_new_table("exchange_rates", "reference", col_desc)
    with db.get_session() as session:
        session.bulk_insert_mappings(
            db.tables["reference"]["exchange_rates"],
            [
                {
                    "country_iso3": d["economy"],
                    "year": int(d["time"][2:]),
                    "exchange_rate": d["value"],
                }
                for d in wb.data.fetch(EXCHANGE_RATE_TABLE)
                if d["value"] is not None
            ],
        )
        session.commit()


def load_ppp_rate(db: DatabaseManager):
    if db.table_exists("ppp", "reference"):
        db.truncate_table("ppp", "reference")
    else:
        col_desc = column_details(
            ["country_iso3", "year", "ppp_rate"], ["country_iso3", "year"]
        )
        db.create_new_table("ppp", "reference", col_desc)
    with db.get_session() as session:
        session.bulk_insert_mappings(
            db.tables["reference"]["ppp"],
            [
                {
                    "country_iso3": d["economy"],
                    "year": int(d["time"][2:]),
                    "ppp_rate": d["value"],
                }
                for d in wb.data.fetch(PPP_RATE_TABLE)
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


def convert_costs_gdp(table, db):
    table_subquery = table.subquery()
    db.map_existing_table("gdp_deflators", "reference")
    db.map_existing_table("currency_exchange_rates", "reference")
    deflator_table = db.tables["reference"]["gdp_deflators"]
    exchange_table = db.tables["reference"]["currency_exchange_rates"]
    max_year = select(func.max(deflator_table.year).label("year")).scalar_subquery()
    latest_year_deflators = (
        select(deflator_table).filter(deflator_table.year == max_year).subquery()
    )
    latest_exchange_rates = (
        select(exchange_table).filter(exchange_table.year == max_year).subquery()
    )
    selected_column_names = [c.name for c in table.selected_columns]
    cost_col_stems = []
    for c in selected_column_names:
        if c.endswith("_cost_millions"):
            col_stem = c.removesuffix("_millions")
            if col_stem in cost_col_stems:
                continue
            val_col_name = c
            cur_col_name = f"{col_stem}_currency"
            yr_col_name = f"{col_stem}_year"
            if (
                cur_col_name in selected_column_names
                and yr_col_name in selected_column_names
            ):
                cost_col_stems.append(col_stem)
    with_latest_deflators = select(
        *table_subquery.columns,
        latest_year_deflators.c.gdp_deflator.label(f"gdp_deflator_latest"),
    ).select_from(
        join(
            table_subquery,
            latest_year_deflators,
            table_subquery.c.country_iso3 == latest_year_deflators.c.country_iso3,
            isouter=True,
        )
    )
    new_table = with_latest_deflators
    for cost_col_stem in cost_col_stems:
        int_table = select(
            *new_table.columns,
            deflator_table.gdp_deflator.label(f"gdp_deflator_{cost_col_stem}"),
        ).select_from(
            join(
                new_table,
                deflator_table,
                and_(
                    new_table.c.country_iso3 == deflator_table.country_iso3,
                    new_table.c[f"{cost_col_stem}_year"] == deflator_table.year,
                ),
                isouter=True,
            )
        )
        new_table = select(
            *int_table.columns,
            latest_exchange_rates.c.exchange_rate.label(
                f"exchange_rate_{cost_col_stem}"
            ),
        ).select_from(
            join(
                int_table,
                latest_exchange_rates,
                int_table.c[f"{cost_col_stem}_currency"]
                == latest_exchange_rates.c.currency,
                isouter=True,
            )
        )
    updated_table = select(
        *new_table.columns,
        *[
            (
                new_table.c[f"{cost_col_stem}_millions"]
                * cast(new_table.c.gdp_deflator_latest, Float)
                * new_table.c[f"exchange_rate_{cost_col_stem}"]
                / new_table.c[f"gdp_deflator_{cost_col_stem}"]
            ).label(f"{cost_col_stem}_usd_gdp_latest_millions")
            for cost_col_stem in cost_col_stems
        ],
    ).select_from(new_table)
    ratio_stems = []
    for cost_col_stem in cost_col_stems:
        if cost_col_stem.startswith("est_"):
            ratio_stems.append(cost_col_stem.removeprefix("est_"))
    return select(
        *updated_table.columns,
        *[
            (
                updated_table.c[f"act_cost_usd_gdp_latest_millions"]
                / updated_table.c[f"est_{ratio_stem}_usd_gdp_latest_millions"]
            ).label(f"{ratio_stem}_usd_gdp_ratio")
            for ratio_stem in ratio_stems
        ],
    )


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
    dur_statement = duration_statement(date_statement)
    dur_ratio_statement = schedule_ratio(dur_statement)
    costs_gdp = convert_costs_gdp(dur_ratio_statement, db)

    with db.get_session() as session:
        session.exec(CreateTableAs(asset_class, schema, costs_gdp))
        session.commit()
        session.exec(
            text(
                f"""ALTER TABLE "{schema}"."{asset_class}" ADD PRIMARY KEY (project_id, sample);"""
            )
        )
        session.commit()
    return


def union_all_tables_in_schema(schema: str, db: DatabaseManager):
    tables = db.get_all_table_names(schema)
    all_columns = set()
    for table in tables:
        db.map_existing_table(table, schema)
    selected_tables = [select(db.tables[schema][table]) for table in tables]
    all_columns.update(c.name for t in selected_tables for c in t.columns)
    union_queries = []
    for table in selected_tables:
        select_columns = [
            table.columns.get(col, literal_column("NULL").label(col)) for col in all_columns
        ]
        union_queries.append(select(*select_columns).select_from(table))
    return select(union(*union_queries).subquery())


def update_prod(db: DatabaseManager, verified: bool = True):
    if db.table_exists(prod_table(verified), "prod"):
        db.drop_table(prod_table(verified), "prod")
    with db.get_session() as session:
        session.exec(
            CreateTableAs(
                prod_table(verified),
                "prod",
                union_all_tables_in_schema(stage_schema(verified), db),
            )
        )
        session.commit()
        session.exec(
            text(
                f"""ALTER TABLE "prod"."{prod_table(verified)}" ADD PRIMARY KEY (project_id, sample);"""
            )
        )
        session.commit()
    return
