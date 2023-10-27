"""
Defines the repository classes to interact with single tables using SQLAlchemy. Inherit from one or multiple of these
classes to use its functionalities.
"""

from abc import ABC
from functools import partial
from typing import Any, Collection, Dict, List

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase, Session

from src.database import SqlAlchemyDb
from src.exc import SqlAlchemyRepositoryQueryingException


class BaseRepository(ABC):
    """Base repository from which all repositories inherit from."""
    _model: DeclarativeBase
    _database: SqlAlchemyDb

    def __init__(self, model: DeclarativeBase, database: SqlAlchemyDb):
        self._model = model
        self._database = database

    def get_database(self) -> SqlAlchemyDb:
        """Returns a reference to the repository's database instance."""
        return self._database


class RetrieveRepository(ABC, BaseRepository):
    """Defines a 'retrieve' method, which queries an object based on its primary key."""

    def retrieve(self, pk: Any, session: Session | None = None) -> Any:
        """
        Queries and returns an object based on its primary key.

        :param pk: The object's primary key identifier.
        :param session: An open database session. If none is given, one will be created and destroyed for this
        operation.
        :return: The object.
        """
        func = partial(self._retrieve, pk=pk)
        if session is None:
            with self.get_database().get_database_session() as session:
                return func(session=session)

        return func(session)

    def _retrieve(self, pk: Any, session: Session) -> Any:
        """Queries and returns an object based on its primary key."""
        item = session.query(self._model).get(pk)
        if item is None:
            raise SqlAlchemyRepositoryQueryingException(f"No object with primary key {pk}, exists on table "
                                                        f"{self._model.__tablename__}")
        return item


class ListRepository(ABC, BaseRepository):
    """Defines a 'list_all' method, which queries all objects of the repository's model."""

    def list_all(self, session: Session | None = None) -> List[Any]:
        """
        Queries and returns all objects.

        :param session: An open database session. If none is given, one will be created and destroyed for this
        operation.
        :return: The list of objects.
        """
        if session is None:
            with self.get_database().get_database_session() as session:
                return session.query(self._model).all()

        return self._list_all(session)


class AddOneRepository(ABC, BaseRepository):
    """Defines an 'add_one' method, which inserts an object."""

    def add_one(self, obj: Any, session: Session | None = None) -> Any:
        """
        Adds, saves and refreshes a single object.

        :param obj: The object to save in the database.
        :param session: An open database session. If none is given, one will be created and destroyed for this
        operation.
        :return: The saved object.
        """
        func = partial(self._add_one, obj=obj)
        if session is None:
            with self.get_database().get_database_session() as session:
                return func(session)

        return func(session)

    @staticmethod
    def _add_one(obj: Any, session: Session) -> Any:
        """Adds, saves and refreshes a single object."""
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj


class AddManyRepository(ABC, BaseRepository):
    """Defines an 'add_many' method, which inserts multiple objects."""

    def add_many(
            self,
            objects: Collection[Any],
            with_return: bool = False,
            session: Session | None = None
    ) -> Collection[Any] | None:
        """
        Adds and saves multiple objects.

        :param objects: The object to save in the database.
        :param with_return: Flag that specifies whether to refresh the collection of objects and return them or not.
        Keep in mind that refreshing a large list of objects might have performance implications.
        :param session: An open database session. If none is given, one will be created and destroyed for this
        operation.
        :return: None if the 'with_return' parameter is False (default). A list with the saved objects otherwise.
        """
        func = partial(self._add_many, objects=objects, with_return=with_return)
        if session is None:
            with self.get_database().get_database_session() as session:
                return func(session=session)

        return func(session=session)

    @staticmethod
    def _add_many(objects: Collection[Any], with_return: bool, session: Session) -> Collection[Any] | None:
        """
        Adds and saves multiple objects. If the 'with_return' parameter is True, the saved objects will be refreshed
        and returned.
        """
        session.add_all(objects)
        session.commit()
        if with_return:
            for obj in objects:
                session.refresh(obj)

            return objects


class DeleteOneRepository(ABC, BaseRepository):
    """Defines a 'delete_one' method, which deletes an object based on its primary key."""

    def delete_one(self, pk: Any, session: Session | None = None) -> None:
        """
        Deletes a single object based on its primary key.

        :param pk: The object's primary key identifier.
        :param session: An open database session. If none is given, one will be created and destroyed for this
        operation.
        :return: None.
        """
        func = partial(self._delete_one, pk=pk)
        if session is None:
            with self.get_database().get_database_session() as session:
                func(session=session)

        func(session=session)

    def _delete_one(self, pk: Any, session: Session) -> None:
        """Deletes a single object based on its primary key."""
        session.query(self._model).get(pk=pk).delete()
        session.commit()


class UpdateRepository(ABC, BaseRepository):
    """
    Defines an 'update_one' method, which updates an object based on a given dictionary of update values, and the
    object's primary key identifier.
    """

    def update_one(self, pk: Any, update_values: Dict, session: Session | None = None) -> Any:
        """
        Updates a single object based on its primary key and a dictionary of update values.

        :param pk: The object's primary key identifier.
        :param update_values: A dictionary with update values. This method assumes the table only has one primary key,
        and, if passed in the update values parameter, it will be ignored.
        :param session: An open database session. If none is given, one will be created and destroyed for this
        operation.
        :return:
        """
        func = partial(self._update_one, pk=pk, update_values=update_values)
        if session is None:
            with self.get_database().get_database_session() as session:
                return func(session=session)

        return func(session=session)

    def _update_one(self, pk: Any, update_values: Dict, session: Session) -> Any:
        """Updates a single object based on its primary key and a dictionary of update values."""
        item: Any = session.query(self._model).get(pk)
        pk_field_name: str = inspect(self._model).primary_key[0].name
        if pk_field_name in update_values:
            update_values.pop(pk_field_name)

        for field, value in update_values.items():
            setattr(item, field, value)

        session.commit()
        session.refresh(item)
        return item
