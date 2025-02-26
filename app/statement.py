from sqlmodel import (
    and_,
    case,
    cast,
    Field,
    select,
    Integer,
    Table,
    Float,
    SQLModel,
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

from app.db import DatabaseManager

STANDARD_DAY = "-07-02"


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


def union_tables(*tables: SQLModel):
    all_columns = set()
    selected_tables = [select(table) for table in tables]
    all_columns.update(c.name for t in selected_tables for c in t.columns)
    return select(
        union(
            *[
                select(
                    [
                        table.columns.get(col, literal_column("NULL").label(col))
                        for col in all_columns
                    ]
                ).select_from(table)
                for table in selected_tables
            ]
        ).subquery()
    )


def union_all_tables_in_schema(schema: str, db: DatabaseManager):
    tables = db.get_all_table_names(schema)
    return union_tables(*[db.tables[schema][table] for table in tables])


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
