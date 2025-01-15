from contextlib import contextmanager
from os import environ
from typing import Annotated, Any, Optional

from fastapi import Depends
from google.cloud.sql.connector import Connector
from pydantic import BaseModel, create_model
from sqlalchemy import inspect, MetaData, Table, Engine
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlmodel import Field, Session, SQLModel, create_engine, select


class Record(BaseModel):
    project_id: str
    sample: str
    data: dict[str, Any]


class CreateTableAs(DDLElement):
    def __init__(self, table_name, schema, selectable):
        self.name = f"{schema}.{table_name}"
        self.selectable = selectable


@compiler.compiles(CreateTableAs, "postgresql")
def compile(element, compiler, **kw):
    return f"""CREATE TABLE {element.name} AS 
    ({compiler.sql_compiler.process(element.selectable, literal_binds=True)});
    """


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
            # echo=True,
        )

    @contextmanager
    def get_session(self):
        with Session(self.engine) as session:
            yield session

    def table_exists(self, table_name: str, schema: str) -> bool:
        return inspect(self.engine).has_table(table_name, schema=schema)

    def schema_exists(self, schema: str) -> bool:
        return inspect(self.engine).has_schema(schema)

    def get_column_descriptions(self, table_name, schema):
        metadata = MetaData(schema)
        metadata.reflect(self.engine)
        table_data = Table(table_name, metadata)
        return {
            c.name: (
                Optional[c.type.python_type] if c.nullable else c.type.python_type,
                Field(default=c.default, primary_key=c.primary_key),
            )
            for c in table_data.columns
        }

    def get_all_table_names(self, schema):
        return [t for t in inspect(self.engine).get_table_names(schema)]

    def map_existing_table(self, table_name: str, schema: str):
        if not (self.table_exists(table_name, schema) and self.schema_exists(schema)):
            print(f"Schema: {schema} does not exist.")
            raise ValueError
        if schema not in self.tables:
            self.tables[schema] = {}
        if table_name in self.tables[schema]:
            return
        self.tables[schema][table_name] = create_model(
            f"{schema}{table_name}",
            __tablename__=table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True},
            __table_args__={"schema": schema, "extend_existing": True},
            **self.get_column_descriptions(table_name, schema),
        )

    def create_new_table(self, table_name: str, schema: str, definitions: dict):
        if self.table_exists(table_name, schema) or (
            schema in self.tables and table_name in self.tables[schema]
        ):
            raise ValueError
        if schema not in self.tables:
            self.tables[schema] = {}
        self.tables[schema][table_name] = create_model(
            f"{schema}{table_name}",
            __tablename__=table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True},
            __table_args__={"schema": schema, "extend_existing": True},
            **definitions,
        )
        SQLModel.metadata.tables[f"{schema}.{table_name}"].create(self.engine)

    def drop_table(self, table_name: str, schema: str):
        if not self.table_exists(table_name, schema):
            print(f"table does not exist: {table_name}")
            raise ValueError
        if self.tables.get(schema, {}).get(table_name) is None:
            self.map_existing_table(table_name, schema)
        try:
            dropped_table = self.tables.get(schema, {}).pop(table_name)
            del dropped_table
            SQLModel.metadata.tables[f"{schema}.{table_name}"].drop(self.engine)
            SQLModel.metadata.remove(SQLModel.metadata.tables[f"{schema}.{table_name}"])
            

        except AttributeError as e:
            print("Something didn't work while dropping table")
            raise

    def select_from_table(self, table_name: str, schema: str) -> list[SQLModel]:
        self.map_existing_table(table_name, schema)
        with self.get_session() as session:
            _tbl = self.tables[schema][table_name]
            data = session.exec(select(_tbl))
            return data.all()

    def select_by_id(
        self, table_name: str, schema: str, project_id: str, sample: str
    ) -> SQLModel:
        self.map_existing_table(table_name, schema)
        _tbl = self.tables[schema][table_name]
        with self.get_session() as session:
            stmt = select(_tbl).where(
                _tbl.project_id == project_id, _tbl.sample == sample
            )
            data = session.exec(stmt)
            return next(data)


database_manager = DatabaseManager()


def get_db_manager():
    yield database_manager


DB_MGMT = Annotated[DatabaseManager, Depends(get_db_manager)]
