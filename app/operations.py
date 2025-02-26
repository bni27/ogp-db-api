import logging
from pathlib import Path

import requests
from sqlmodel import select, text, union
import wbgapi as wb

from app.column import column_details
from app.db import CreateTableAs, DatabaseManager
from app.exception import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.filesys import get_data_files, read_raw_data_file
from app.schema import prod_table, raw_schema, stage_schema
from app.statement import (
    convert_costs_gdp,
    date_year_statement,
    duration_statement,
    schedule_ratio,
    union_all_tables_in_schema,
)


EXCHANGE_RATE_TABLE = "PA.NUS.FCRF"
PPP_RATE_TABLE = "PA.NUS.PPP"

STANDARD_DAY = "-07-02"


_logger = logging.getLogger(__name__)


def load_raw_data_from_file(
    file_path: Path, table_name: str, schema: str, db: DatabaseManager
):
    headers, records = read_raw_data_file(file_path)
    if db.table_exists(table_name, schema):
        db.drop_table(table_name, schema)
    db.create_new_table(table_name, schema, column_details(headers))
    db.load_data_into_table(table_name, schema, records)


def load_gdp_deflators(db: DatabaseManager):
    if db.table_exists("gdp_deflators", "reference"):
        db.truncate_table("gdp_deflators", "reference")
    else:
        db.create_new_table(
            "gdp_deflators",
            "reference",
            column_details(
                ["country_iso3", "year", "gdp_deflator"], ["country_iso3", "year"]
            ),
        )
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
    db.load_data_into_table(
        "gdp_deflators",
        "reference",
        [
            {
                "country_iso3": d["Country"],
                "year": d["Time"][2:],
                "gdp_deflator": d["value"],
            }
            for d in data
        ],
    )


def load_exchange_rate(db: DatabaseManager):
    if db.table_exists("exchange_rates", "reference"):
        db.truncate_table("exchange_rates", "reference")
    else:
        db.create_new_table(
            "exchange_rates",
            "reference",
            column_details(
                ["country_iso3", "year", "exchange_rate"], ["country_iso3", "year"]
            ),
        )
    db.load_data_into_table(
        "exchange_rates",
        "reference",
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


def load_ppp_rate(db: DatabaseManager):
    if db.table_exists("ppp", "reference"):
        db.truncate_table("ppp", "reference")
    else:
        db.create_new_table(
            "ppp",
            "reference",
            column_details(
                ["country_iso3", "year", "ppp_rate"], ["country_iso3", "year"]
            ),
        )
    db.load_data_into_table(
        "ppp",
        "reference",
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
