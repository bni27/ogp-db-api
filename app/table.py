from os import environ
from typing import Annotated, Optional

from fastapi import Depends
from google.cloud.sql.connector import Connector
from pydantic import create_model
from sqlalchemy import inspect, MetaData, Table, Engine
from sqlmodel import Field, Session, SQLModel, create_engine


# build connection (for creator argument of connection pool)
def getconn():
    # Cloud SQL Python Connector object
    with Connector() as connector:
        conn = connector.connect(
            environ.get("INSTANCE_CONNECTION_NAME"),
            "pg8000",
            user=environ.get("DB_USER"),
            password=environ.get("DB_PASS"),
            db=environ.get("DB_NAME"),
        )
    return conn


class DatabaseManager:
    tables: dict[str, dict[str, SQLModel]]
    engine: Engine

    def __init__(self):
        self.tables = {}
        self.engine = create_engine(
            "postgresql+pg8000://",
            creator=getconn,
            echo=True,
        )

    def get_session(self):
        with Session(self.engine) as session:
            yield session

    def table_exists(self, table_name: str, schema: str) -> bool:
        return inspect(self.engine).has_table(f"{schema}.{table_name}")

    def schema_exists(self, schema: str) -> bool:
        return inspect(self.engine).has_schema(schema)

    def get_column_descriptions(self, table_name, schema):
        metadata = MetaData()
        metadata.reflect(self.engine)
        table_data = Table(f"{schema}.{table_name}", metadata)
        return {
            c.name: (
                Optional[c.type.python_type] if c.nullable else c.type.python_type,
                Field(default=c.default, primary_key=c.primary_key),
            )
            for c in table_data.columns
        }

    def map_existing_table(self, table_name: str, schema: str):
        if not (self.table_exists(table_name, schema) and self.schema_exists(schema)):
            raise ValueError
        if schema not in self.tables:
            self.tables[schema] = {}
        if table_name in self.tables[schema]:
            return
        new_table = create_model(
            table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True, "__table_args__": {"schema": schema}},
            **self.get_column_descriptions(table_name, schema),
        )
        self.tables[schema][table_name] = new_table

    def map_new_table(self, table_name: str, schema: str, definitions: dict):
        if self.table_exists(table_name, schema) or (
            schema in self.tables and table_name in self.tables[schema]
        ):
            raise ValueError

        new_table = create_model(
            table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True, "__table_args__": {"schema": schema}},
            **definitions,
        )
        self.tables[table_name] = new_table
        new_table.__table__.create(self.engine)


database_manager = DatabaseManager()


def get_db_manager():
    yield database_manager


DB_MGMT = Annotated[DatabaseManager, Depends(get_db_manager)]
