"""
RasgoQL Custom Errors
"""

class RasgoQLWarning(Warning):
    """
    Base warning attributable to RasgoQL code
    """
    pass

class RasgoQLException(Exception):
    """
    Base error attributable to RasgoQL code
    """
    pass

class DWCredentialsWarning(RasgoQLException):
    """
    Missing DataWarehouse credentials
    """
    pass

class DWConnectionError(RasgoQLException):
    """
    Error from DataWarehouse connection activity
    """
    pass

class DWQueryError(RasgoQLException):
    """
    Error from DataWarehouse query activity
    """
    pass

class ParameterException(RasgoQLException):
    """
    Problem populating a value-restricted parameter
    """
    pass

class PackageDependencyWarning(RasgoQLException):
    """
    Problem importing a required package dependency
    """
    pass

class SQLWarning(RasgoQLException):
    """
    Possible problem with SQL text
    """
    pass

class TableAccessError(RasgoQLException):
    """
    Error accessing Table
    """
    pass

class TableConflictException(RasgoQLException):
    """
    Possiblle conflict with Table names
    """
    pass

class TransformRenderingError(RasgoQLException):
    """
    Error from Transform rendering activity
    """
    pass
