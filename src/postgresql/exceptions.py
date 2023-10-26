"""Custom exceptions."""


class PostgreSqlDbBuildingException(Exception):
    """Exception to raise when building a PostgreSqlDb string connection using the PostgreSqlDbBuilder class."""


class PostgreSqlQueryingException(Exception):
    """Exception to raise when executing any query using one of the PostgreSqlDb repository methods."""
