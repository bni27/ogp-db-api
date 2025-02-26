from datetime import date, datetime
from typing import Optional

from sqlmodel import Field


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
DATE_FORMAT = "%Y-%m-%d"
PRIMARY_KEYS = {"project_id", "sample"}


def column_details(
    headers: list[str],
    primary_keys: list[str] = PRIMARY_KEYS,
) -> dict[str, tuple[type, Field]]:
    details = {}
    for header in headers:
        pk = header in primary_keys
        dtype = data_type(header)
        details[header] = (
            dtype if pk else Optional[dtype],
            Field(default="" if pk else None, primary_key=pk),
        )
    return details


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
