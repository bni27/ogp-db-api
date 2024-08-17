from string import ascii_lowercase
from typing import Generator


PRIMARY_KEYS = ["project_id", "sample"]

COLUMN_TYPE_NOTATION = {
    "INTEGER": {"suffixes": ["_year"], "prefixes": []},
    "BOOLEAN": {"suffixes": [], "prefixes": ["is_"]},
    "DATE": {"suffixes": ["_date"], "prefixes": []},
    "FLOAT": {
        "suffixes": ["_millions", "_value", "_ratio", "_duration", "_thousands"],
        "prefixes": [],
    },
    "VARCHAR": {"suffixes": [""], "prefixes": []},
}

STANDARD_DAY = "-07-02"


class AliasFactory:
    _aliases: Generator[str, None, None]
    _previous: str | None

    def __init__(self):
        self._aliases = (a for a in ascii_lowercase)

    @property
    def value(self):
        try:
            return next(self._aliases)
        except StopIteration:
            self._aliases = (a for a in ascii_lowercase)
            return next(self._aliases)


ALIASES = AliasFactory()


def _verified_name(verified: bool = False) -> str:
    return "verified" if verified else "unverified"


def raw_schema(verified: bool = False) -> str:
    return f"raw_{_verified_name(verified)}"


def stage_schema(verified: bool = False) -> str:
    return f"stage_{_verified_name(verified)}"


def prod_table(verified: bool = False) -> str:
    return f"{_verified_name(verified)}_projects"


def column_tuples(column_names: list[str]) -> list[tuple[str, str]]:
    columns: list[tuple[str, str]] = []
    for column_name in column_names:
        lower_col = column_name.lower()
        for dtype, conditions in COLUMN_TYPE_NOTATION.items():
            if any(
                lower_col.endswith(suffix) for suffix in conditions["suffixes"]
            ) or any(lower_col.endswith(prefix) for prefix in conditions["prefixes"]):
                columns.append((lower_col, dtype))
                break

    return columns


def gen_col_str(
    columns: list[tuple[str, str]],
    primary_key: str | list[str] | None = None,
) -> str:
    if primary_key is None:
        primary_key = [columns[0][0]]
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    string_pieces = []
    for column in columns:
        column_repr = " ".join(column)
        column_repr += " NOT NULL" if column[0] in primary_key else ""
        string_pieces.append(column_repr)
    string_pieces.append(f"PRIMARY KEY ({', '.join(primary_key)})")
    return ", ".join(string_pieces)


def drop_table_statement(
    table_name: str,
    schema: str,
    if_exists: bool = True,
) -> str:
    return f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{schema}.{table_name}"


def create_table_statement(
    table_name: str,
    columns: list[str],
    schema: str | None = None,
    primary_key: list[str] | str = PRIMARY_KEYS,
) -> str:
    table_str = table_name if schema is None else f"{schema}.{table_name}"
    col_str = gen_col_str(columns=column_tuples(columns), primary_key=primary_key)
    statement = f"""CREATE TABLE IF NOT EXISTS {table_str}
        ({col_str})
        """
    print(statement)
    return statement


def copy_statement(table_name: str, header_line: str, schema) -> str:
    return f"COPY {schema}.{table_name}({header_line}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"


def select_statement(
    table_name: str,
    columns: str | list[str] = "*",
    schema: str | None = None,
    where: str | None = None,
    limit: int | None = None,
):
    table_string = table_name if schema is None else f"{schema}.{table_name}"
    col_str = ", ".join(columns) if isinstance(columns, list) else columns
    query = f"SELECT {col_str} FROM {table_string}"
    query += "" if where is None else f" {where}"
    query += "" if limit is None else f" LIMIT {limit}"
    return f"{query}"


def union_statement(*statements: str) -> str:
    return " UNION ".join(statements)


def join_statement(
    exp_1: str,
    exp_2: str,
    on: str,
    chained: bool = False,
) -> tuple[str, dict[str, str]]:
    alias_map = {}
    if chained:
        join_str = f"{exp_1}"
    else:
        alias_map[exp_1] = ALIASES.value
        f"({exp_1}) AS {alias_map[exp_1]}"

    alias_map[exp_2] = ALIASES.value
    join_str += f" JOIN ({exp_2}) AS {alias_map[exp_2]} ON {on}"
    return join_str, alias_map


def case_if_year_and_date(column_stem: str) -> str:
    return f"""CASE WHEN {column_stem}_year IS NULL 
    THEN DATE_PART('year', {column_stem}_date) ELSE {column_stem}_year 
    END AS {column_stem}_year"""


def date_from_year(
    year_col: str, year_literal: int | None = None, _mm_dd: str = STANDARD_DAY
) -> str:
    if year_literal is None:
        return f"(CASE WHEN {year_col} is not NULL THEN date(concat({year_col}, '{_mm_dd}')) ELSE NULL END)"
    return f"date(concat({year_literal}, '{_mm_dd}'))"


def duration_statements(col_stem: str) -> str:
    start_year = f"start_{col_stem}_year"
    start_date = f"start_{col_stem}_date"
    col_sets: list[dict[str, str]] = [
        {
            "date": f"est_{col_stem}_completion_date",
            "year": f"est_{col_stem}_completion_year",
            "dur": f"est_{col_stem}_duration",
        },
        {
            "date": "act_completion_date",
            "year": "act_completion_year",
            "dur": f"act_{col_stem}_duration",
        },
    ]
    _duration_statements = []
    for col_set in col_sets:
        end_date = col_set["date"]
        end_year = col_set["year"]
        duration_col = col_set["dur"]
        _duration_statements.append(
            f"""CASE WHEN {duration_col} is NULL THEN (
            (CASE WHEN {end_date} is NULL THEN {date_from_year(end_year)} ELSE {end_date} END)
            - (CASE WHEN {start_date} is NULL THEN {date_from_year(start_year)} ELSE {start_date} END)
            )::FLOAT / 365
            ELSE {duration_col} END as {duration_col}"""
        )
    return ", ".join(_duration_statements)
