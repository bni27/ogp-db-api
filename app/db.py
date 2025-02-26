from contextlib import contextmanager
import logging
from os import environ
from typing import Annotated, Any, Optional

from fastapi import Depends
from google.cloud.sql.connector import Connector
from pydantic import BaseModel, create_model
from sqlalchemy import inspect, MetaData, Table, Engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlmodel import (
    Column,
    create_engine,
    delete,
    Field,
    select,
    Session,
    SQLModel,
    String,
)

from app.column import column_details


PRIMARY_KEYS = ["project_id", "sample"]
_logger = logging.getLogger(__name__)


class Record(BaseModel):
    project_id: str
    sample: str
    data: dict[str, Any]


class CreateTableAs(DDLElement):
    def __init__(self, table_name, schema, selectable):
        self.name = f"{schema}.{table_name}"
        self.selectable = selectable


@compiler.compiles(CreateTableAs, "postgresql")
def compile(element, compiler, **kw) -> str:
    """Compile the CreateTableAs statement for PostgreSQL.

    Args:
        element: The CreateTableAs element.
        compiler: The SQLAlchemy compiler.
        **kw: Additional keyword arguments.

    Returns:
        str: The compiled SQL statement.
    """
    return f"""CREATE TABLE {element.name} AS 
    ({compiler.sql_compiler.process(element.selectable, literal_binds=True)});
    """


def getconn():
    """Get a connection to the database using the Cloud SQL Python Connector.

    Returns:
        Engine: The SQLAlchemy engine.
    """
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
        """Initialize the DatabaseManager."""
        self.tables = {}
        self.engine = create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )

    @contextmanager
    def get_session(self):
        """Get a session for the database.

        Yields:
            Session: The SQLAlchemy session.
        """
        with Session(self.engine) as session:
            yield session

    def table_exists(self, table_name: str, schema: str) -> bool:
        """
        Check if a table exists in the given schema.

        Args:
            table_name (str): The name of the table.
            schema (str): The schema name.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        return inspect(self.engine).has_table(table_name, schema=schema)

    def schema_exists(self, schema: str) -> bool:
        """
        Check if a schema exists in the database.

        Args:
            schema (str): The schema name.

        Returns:
            bool: True if the schema exists, False otherwise.
        """
        return inspect(self.engine).has_schema(schema)

    def get_columns(self, table_name: str, schema: str):
        """
        Get column names for a table in the given schema.

        Args:
            table_name (str): The name of the table.
            schema (str): The schema name.

        Returns:
            list: A list of column names.
        """
        metadata = MetaData(schema)
        metadata.reflect(self.engine)
        table_data = Table(table_name, metadata)
        return table_data.columns

    def get_column_descriptions(self, table_name, schema, primary_keys: list[str] = PRIMARY_KEYS):
        """
        Get column descriptions for a table in the given schema.

        Args:
            table_name (str): The name of the table.
            schema (str): The schema name.

        Returns:
            Dict[str, Optional[type]]: A dictionary of column descriptions.
        """
        details = {
            c.name: (
                Optional[c.type.python_type] if c.nullable else c.type.python_type,
                (
                    Field(
                        default=None,
                        sa_column=Column(postgresql.ARRAY(String())),
                    )
                    if c.type.python_type == list
                    else Field(default=c.default, primary_key=(c.primary_key))
                ),
            )
            for c in self.get_columns(table_name, schema)
        }
        for pk in primary_keys:
            if pk in details:
                details[pk][1].primary_key = True
        return details

    def get_all_table_names(self, schema):
        """
        Retrieve all table names from the specified schema.

        Args:
            schema (str): The name of the schema from which to retrieve table names.

        Returns:
            list: A list of table names within the specified schema.
        """
        return [t for t in inspect(self.engine).get_table_names(schema)]

    def map_existing_table(self, table_name: str, schema: str):
        """
        Maps an existing table to the internal tables dictionary if it exists.

        This method checks if the specified table exists within the given schema.
        If the table and schema exist, it creates a model for the table and adds it
        to the internal tables dictionary. If the schema or table does not exist,
        it logs an error and raises a ValueError.

        Args:
            table_name (str): The name of the table to map.
            schema (str): The schema in which the table resides.

        Raises:
            ValueError: If the schema does not exist or the table does not exist within the schema.
        """
        if not self.schema_exists(schema):
            _logger.error(f"Schema {schema} does not exist")
            raise ValueError
        if schema not in self.tables:
            self.tables[schema] = {}
        if table_name in self.tables.get(schema, {}):
            return self.tables[schema][table_name]
        if not self.table_exists(table_name, schema):
            _logger.error(f"Table {table_name} does not exist in schema {schema}")
            raise ValueError
        self.tables[schema][table_name] = create_model(
            f"{schema}{table_name}",
            __tablename__=table_name,
            __base__=SQLModel,
            __cls_kwargs__={"table": True},
            __table_args__={"schema": schema, "extend_existing": True},
            **self.get_column_descriptions(table_name, schema),
        )
        return self.tables[schema][table_name]

    def create_new_table(self, table_name: str, schema: str, definitions: dict):
        """
        Creates a new table in the specified schema with the given definitions.

        Args:
            table_name (str): The name of the table to be created.
            schema (str): The schema in which the table will be created.
            definitions (dict): A dictionary containing the column definitions for the table.

        Raises:
            ValueError: If the table already exists in the specified schema.

        Returns:
            None
        """
        if self.table_exists(table_name, schema) or (
            schema in self.tables and table_name in self.tables[schema]
        ):
            _logger.error(f"Table {table_name} already exists in schema {schema}")
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
        return self.tables[schema][table_name]

    def drop_table(self, table_name: str, schema: str):
        """
        Drops a table from the database if it exists.

        Args:
            table_name (str): The name of the table to be dropped.
            schema (str): The schema where the table is located.

        Raises:
            ValueError: If the table does not exist.
            AttributeError: If there is an issue while dropping the table.

        Notes:
            - Checks if the table exists before attempting to drop it.
            - Maps the existing table if it is not already mapped.
            - Removes the table from the internal tables dictionary and SQLModel metadata.
        """
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

    def truncate_table(self, table_name: str, schema: str):
        """
        Truncates a table in the database if it exists.

        Args:
            table_name (str): The name of the table to be truncated.
            schema (str): The schema where the table is located.

        Raises:
            ValueError: If the table does not exist.

        Notes:
            - Checks if the table exists before attempting to truncate it.
            - Maps the existing table if it is not already mapped.
        """
        if not self.table_exists(table_name, schema):
            raise ValueError
        self.map_existing_table(table_name, schema)
        with self.get_session() as session:
            session.exec(delete(self.tables[schema][table_name]))
            session.commit()

    def select_from_table(self, table_name: str, schema: str) -> list[SQLModel]:
        """
        Selects all records from a specified table within a given schema.

        Args:
            table_name (str): The name of the table to select from.
            schema (str): The schema where the table is located.

        Returns:
            list[SQLModel]: A list of SQLModel instances representing the records in the table.
        """
        self.map_existing_table(table_name, schema)
        _tbl = self.tables[schema][table_name]
        with self.get_session() as session:
            data = session.exec(select(_tbl))
            return data.all()

    def select_by_id(
        self, table_name: str, schema: str, project_id: str, sample: str
    ) -> SQLModel:
        """
        Select a record from the specified table by project ID and sample.

        Args:
            table_name (str): The name of the table to query.
            schema (str): The schema of the table.
            project_id (str): The project ID to filter by.
            sample (str): The sample to filter by.

        Returns:
            SQLModel: The selected record from the table.

        Raises:
            StopIteration: If no record is found matching the criteria.
        """
        self.map_existing_table(table_name, schema)
        _tbl = self.tables[schema][table_name]
        with self.get_session() as session:
            stmt = select(_tbl).where(
                _tbl.project_id == project_id, _tbl.sample == sample
            )
            data = session.exec(stmt)
            return next(data)

    def load_data_into_table(
        self,
        table_name: str,
        schema: str,
        records: list[dict[str, Any]],
    ):
        """
        Load data into a table in the database.

        Args:
            table_name (str): The name of the table to load data into.
            schema (str): The schema of the table.
            records (list[dict[str, Any]]): The records to load into the table,
                    records are in dict format, with headers as keys.
        Returns:
            None
        """
        _tbl = self.map_existing_table(table_name, schema)
        with self.get_session() as session:
            session.bulk_insert_mappings(
                _tbl,
                records,
            )
            session.commit()


database_manager = DatabaseManager()


def get_db_manager():
    yield database_manager


DB_MGMT = Annotated[DatabaseManager, Depends(get_db_manager)]
