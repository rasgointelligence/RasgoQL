"""
RasgoQL Custom Errors
"""


class RasgoQLWarning(Warning):
    """
    Base warning attributable to RasgoQL code
    """


class RasgoQLException(Exception):
    """
    Base error attributable to RasgoQL code
    """


class DWCredentialsWarning(RasgoQLException):
    """
    Missing DataWarehouse credentials
    """


class DWConnectionError(RasgoQLException):
    """
    Error from DataWarehouse connection activity
    """


class DWQueryError(RasgoQLException):
    """
    Error from DataWarehouse query activity
    """


class ParameterException(RasgoQLException):
    """
    Problem populating a value-restricted parameter
    """


class PackageDependencyWarning(RasgoQLException):
    """
    Problem importing a required package dependency
    """


class SQLWarning(RasgoQLException):
    """
    Possible problem with SQL text
    """


class TableAccessError(RasgoQLException):
    """
    Error accessing Table
    """


class TableConflictException(RasgoQLException):
    """
    Possiblle conflict with Table names
    """


class TransformRenderingError(RasgoQLException):
    """
    Error from Transform rendering activity
    """
