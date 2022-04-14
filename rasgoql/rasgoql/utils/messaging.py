"""
Verbose messaging functions
"""
import logging

rql_run_verbose: bool = False


def verbose_message(
    message: str,
    logger: logging.Logger,
) -> None:
    """
    Logs messages only if verbose flag is set to true
    """
    logger.setLevel(logging.INFO)
    if rql_run_verbose:
        logger.info(message)


def set_verbose(
    value: bool,
) -> None:
    """
    Set global var
    """
    global rql_run_verbose
    rql_run_verbose = value
