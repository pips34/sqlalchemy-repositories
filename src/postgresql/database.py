"""
Defines the PostgreSqlDb class and its builder: PostgreSqlDbBuilder.

The PostgreSqlDb class is meant to be instantiated and used throughout a project's execution.
It provides access to all the needed parts of a SQLAlchemy connection:
- Connection URL
- The SQLAlchemy DeclarativeBase
- SQLAlchemy engine
- The SQLAlchemy MetaData
- The SQLAlchemy SessionMaker

The PostgreSqlDbBuilder is meant to be instantiated, and all of its parameters should be set using the methods
'set_xyz', which are (with its default values in front of them):
- set_host              localhost
- set_username          postgres
- set_port              5432
_ set_schema            public
- set_password
- set_database_name
"""

from contextlib import contextmanager
from typing import Any, List, Self
from urllib.parse import quote_plus

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker, session

from src.postgresql.exceptions import PostgreSqlDbBuildingException


class PostgreSqlDb:
    """
    The PostgreSqlDb class is meant to be instantiated and used throughout a project's execution.
    It provides access to all the needed parts of a SQLAlchemy connection:
    - Connection URL
    - The SQLAlchemy DeclarativeBase
    - SQLAlchemy engine
    - The SQLAlchemy MetaData
    - The SQLAlchemy SessionMaker
    """
    _postgresql_url: str
    _Base: Any
    _engine: Engine
    _MetaData: Any
    _SessionMaker: sessionmaker

    def __init__(self, postgresql_url, postgresql_schema: str = "public"):
        self._postgresql_url = postgresql_url
        try:
            self._engine = create_engine(url=self._postgresql_url)
            self._SessionMaker = sessionmaker(bind=self._engine)
            self._Base = declarative_base(metadata=MetaData(schema=postgresql_schema))
            self._MetaData = self._Base.metadata
        except Exception as e:
            raise PostgreSqlDbBuildingException(e)

    def get_metadata(self) -> Any:
        """
        Returns the metadata. Turns useful when using Alembic to manage migrations.

        :return: The declarative base's metadata.
        """
        return self._MetaData

    def get_base(self) -> Any:
        """
        Returns the declarative base.

        :return: The declarative base itself.
        """
        return self._Base

    def create_tables(self):
        """Creates all tables associated with the class's metadata."""
        self._MetaData.create_all(bind=self._engine)

    @contextmanager
    def get_database_session(self) -> Session:
        """
        Allows for the creation of database session to begin transactions.

        :return: An instantiated Session Maker.
        """
        db_session: session.Session = self._SessionMaker()
        try:
            yield db_session
        finally:
            db_session.close()


class PostgreSqlDbBuilder:
    """
    The PostgreSqlDbBuilder is meant to be instantiated, and all of its parameters should be set using the methods
    'set_xyz', which are (with its default values in front of them):
    - set_host              localhost
    - set_username          postgres
    - set_port              5432
    _ set_schema            public
    - set_password
    - set_database_name
    """
    _postgresql_host: str = "localhost"
    _postgresql_username: str = "postgres"
    _postgresql_schema: str = "public"
    _postgresql_port: int = 5432
    _postgresql_password: str
    _postgresql_database_name: str

    def set_host(self, postgresql_host: str) -> Self:
        """
        Sets the PostgreSQL Host and returns the builder.

        :param postgresql_host: The host as string, for example 'localhost'.
        :return: The builder instance (self).
        """
        self._postgresql_host = postgresql_host
        return self

    def set_username(self, postgresql_username: str) -> Self:
        """
        Sets the PostgreSQL Username and returns the builder.

        :param postgresql_username: The PostgreSQL database username as a string.
        :return: The builder instance (self).
        """
        self._postgresql_username = postgresql_username
        return self

    def set_password(self, postgresql_password: str) -> Self:
        """
        Sets the PostgreSQL Password and returns the builder.

        :param postgresql_password: The PostgreSQL database user's password as a string.
        :return: The builder instance (self).
        """
        self._postgresql_password = postgresql_password
        return self

    def set_port(self, postgresql_port: int) -> Self:
        """
        Sets the server port and returns the builder.

        :param postgresql_port: The server's port number, an integer between 1 and 65535.
        :return: The builder instance (self).
        """
        self._postgresql_port = postgresql_port
        return self

    def set_database_name(self, postgresql_database_name: str) -> Self:
        """
        Sets the database name and returns the builder.

        :param postgresql_database_name: The database name as a string.
        :return: The builder instance (self).
        """
        self._postgresql_database_name = postgresql_database_name
        return self

    def set_schema(self, postgresql_database_schema: str) -> Self:
        """
        Sets the PostgreSQL database schema and returns the builder.

        :param postgresql_database_schema: The database schema as a string.
        :return: The builder instance (self).
        """
        self._postgresql_schema = postgresql_database_schema
        return self

    def build(self) -> PostgreSqlDb:
        """
        With all the set attribute values, or the PostgreSQL default values, this method builds an instance of the
        PostgreSqlDb class.

        :return: The instance of a PostgreSqlDb class with the attributes set in the builder.
        """
        try:
            url: bytes = (b"postgresql+psycopg2://" +  # noqa
                          (f"{self._postgresql_username}:%s" % quote_plus(self._postgresql_password)).encode() + b"@" +
                          self._postgresql_host.encode() + b":" +
                          str(self._postgresql_port).encode() + b"/" +
                          self._postgresql_database_name.encode()).decode()

            if self._postgresql_schema is None:
                return PostgreSqlDb(url)
            else:
                return PostgreSqlDb(url, self._postgresql_schema)

        except PostgreSqlDbBuildingException:
            raise

        except AttributeError:
            unset_fields: List[Any] = []
            if not hasattr(self, "_postgresql_password"):
                unset_fields.append("_postgresql_password")
            if not hasattr(self, "_postgresql_database_name"):
                unset_fields.append("_postgresql_database_name")
            raise PostgreSqlDbBuildingException(f"The following fields were not set before building: {unset_fields}.")

        except Exception as e:
            raise PostgreSqlDbBuildingException(e)
