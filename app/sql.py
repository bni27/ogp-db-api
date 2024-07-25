PRIMARY_KEYS = ["project_id", "sample"]

COLUMN_TYPE_NOTATION = {
    "INTEGER": {"suffixes": ["_year"], "prefixes": []},
    "BOOLEAN": {"suffixes": [], "prefixes":["is_"]},
    "DATE": {"suffixes": ["_date"], "prefixes":[]},
    "FLOAT": {"suffixes": ["_millions", "_value", "_ratio"], "prefixes":[]},
    "VARCHAR": {"suffixes": [""], "prefixes": []}
}


def _verified_name(verified: bool = False) -> str:
    return "verified" if verified else "unverified"


def raw_schema(verified: bool = False) -> str:
    return f"raw_{_verified_name(verified)}"


def stage_schema(verified: bool = False) -> str:
    return f"stage_{_verified_name(verified)}"


def column_tuples(column_names: list[str]) -> list[tuple[str, str]]:
    columns: list[tuple[str, str]] = []
    for column_name in column_names:
        lower_col = column_name.lower()
        for dtype, conditions in COLUMN_TYPE_NOTATION.items():
            if (
                any(lower_col.endswith(suffix) for suffix in conditions["suffixes"])
                or 
                any(lower_col.endswith(prefix) for prefix in conditions["prefixes"])
            ):
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
        column_repr = ' '.join(column)
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
        ( {col_str} )
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
    col_str = ', '.join(columns) if isinstance(columns, list) else columns
    query = f"SELECT {col_str} FROM {table_string}"
    query += "" if where is None else f" {where}"
    query += "" if limit is None else f" LIMIT {limit}"
    return query + ";"


def union_statement(*statements: str) -> str:
    print("THIS IS MY LOG FOR UNION STATEMENT")
    print(statements)
    return " UNION ".join(statements)
