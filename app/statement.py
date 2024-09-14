from __future__ import annotations
from string import ascii_lowercase
from typing import Generator


class AliasFactory:
    _aliases: Generator[str, None, None]
    _previous: str | None

    def __init__(self):
        self._aliases = (a for a in ascii_lowercase)
        self._previous = None

    @property
    def previous(self) -> str | None:
        return self._previous

    @previous.setter
    def previous(self, value):
        self._previous = value

    @property
    def new(self):
        try:
            self.previous = next(self._aliases)
        except StopIteration:
            self._aliases = (a for a in ascii_lowercase)
            self.previous = next(self._aliases)
        finally:
            return self.previous


class Query:
    aliases: AliasFactory
    table: str | None
    columns: list[tuple[str, str]]
    select: list[str] | None
    subquery: Query | None
    limit: int | None
    where: str | None

    def __init__(
        self,
        source: str | Query,
        columns: list[tuple[str, str]],
        *,
        select: str | None = None,
        limit: int | None = None,
        where: str | None = None,
    ):
        self.aliases = AliasFactory()
        self.table = source if isinstance(source, str) else None
        self.subquery = source if isinstance(source, Query) else None
        self.columns = columns
        self.source = select
        self.limit = limit
        self.where = where

    @property
    def select_statement(self) -> str:
        _statement = f"SELECT {', '.join(self.select)} FROM "
        _statement += (
            f"({self.subquery.select_statement}) as {self.aliases.new}"
            if self.subquery is not None
            else f"{self.table} as {self.aliases.new}"
        )
        if self.where is not None:
            _statement += f" "