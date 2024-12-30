from os import environ
from typing import Optional

from google.cloud.sql.connector import Connector
from pydantic import create_model
from sqlalchemy import inspect, MetaData, Table
from sqlmodel import Field, Session, SQLModel, create_engine


# build connection (for creator argument of connection pool)
def getconn():
    # Cloud SQL Python Connector object
    with Connector() as connector:
        conn = connector.connect(
            f"/cloudsql/{environ.get('INSTANCE_CONNECTION_NAME')}",  # Cloud SQL instance connection name
            "pg8000",
            user=environ.get("DB_USER"),
            password=environ.get("DB_PASS"),
            db=environ.get("DB_NAME"),
        )
    return conn


engine = create_engine(
    "postgresql+pg8000",
    creator=getconn,
    echo=True,
)


def get_session():
    with Session(engine) as session:
        yield session


def table_exists(table_name: str) -> bool:
    return inspect(engine).has_table(table_name)


def get_column_descriptions(table_name, engine):
    metadata = MetaData()
    metadata.reflect(engine)
    table_data = Table(table_name, metadata)
    return {
        c.name: (
            Optional[c.type.python_type] if c.nullable else c.type.python_type,
            Field(default=c.default, primary_key=c.primary_key),
        )
        for c in table_data.columns
    }


class Factory:
    tables: dict[str, SQLModel]

    def __init__(self):
        self.tables = {}

    def map_existing_table(self, table_name: str):
        if table_name in self.tables:
            raise ValueError
        if not table_exists(table_name):
            raise ValueError
        new_table = create_model(
            table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True},
            **get_column_descriptions(table_name, engine),
        )
        self.tables[table_name] = new_table

    def map_new_table(self, table_name: str, definitions: dict):
        if table_name in self.tables:
            raise ValueError
        if table_exists(table_name):
            raise ValueError
        new_table = create_model(
            table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True},
            **definitions,
        )
        self.tables[table_name] = new_table
        new_table.__table__.create(engine)
