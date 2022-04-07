"""
Verbose messaging functions
"""
import logging

RQL_RUN_VERBOSE: bool = False

def verbose_message(
        message: str,
        logger: logging.Logger
    ) -> None:
    """
    Logs messages only if verbose flag is set to true
    """
    logger.setLevel(logging.INFO)
    if RQL_RUN_VERBOSE:
        logger.info(message)

def set_verbose(
        value: bool
    ) -> None:
    """
    Set global var
    """
    global RQL_RUN_VERBOSE
    RQL_RUN_VERBOSE = value
