"""
Defines the SqlAlchemyDb class. This class is meant to be instantiated and used throughout a project's execution.
It provides access to all the needed parts of a SQLAlchemy connection:
- Connection URL
- The SQLAlchemy DeclarativeBase
- SQLAlchemy engine
- The SQLAlchemy MetaData
- The SQLAlchemy SessionMaker
"""

from contextlib import contextmanager
from typing import Any

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker, session


class SqlAlchemyDb:
    """
    The SqlAlchemyDb class is meant to be instantiated and used throughout a project's execution.
    It provides access to all the needed parts of a SQLAlchemy connection:
    - Connection URL
    - The SQLAlchemy DeclarativeBase
    - SQLAlchemy engine
    - The SQLAlchemy MetaData
    - The SQLAlchemy SessionMaker
    """
    _connection_url: str
    _Base: Any
    _engine: Engine
    _MetaData: Any
    _SessionMaker: sessionmaker

    def __init__(self, connection_url, schema: str = "public"):
        self._connection_url = connection_url
        self._engine = create_engine(url=self._connection_url)
        self._SessionMaker = sessionmaker(bind=self._engine)
        self._Base = declarative_base(metadata=MetaData(schema=schema))
        self._MetaData = self._Base.metadata

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
