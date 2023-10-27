"""Custom exceptions."""


class SqlAlchemyRepositoryQueryingException(Exception):
    """Exception to raise when executing any query using one of the PostgreSqlDb repository methods."""
