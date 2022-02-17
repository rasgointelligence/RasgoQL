"""
DataWarehouse credential functions
"""
import os
import logging
from pathlib import Path
import yaml

import dotenv

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_env(
        filepath: str = None
    ):
    """
    Loads a .env file, allowing contents to be callable by os.getenv()
    """
    dotenv.load_dotenv(filepath)

def load_yml(
        filepath: str
    ):
    """
    Loads a yml file and returns the contents
    """
    with open(filepath) as yml_file:
        contents = yaml.load(yml_file, Loader=yaml.Loader)
    return contents

def read_file(
        filepath: str = None
    ) -> str:
    """
    Reads the contents of a file
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f'File {filepath} does not exist')
    with open(filepath, "r") as _file:
        contents = _file.read()
    return contents

def save_env(
        creds: str,
        filepath: str = None,
        overwrite: bool = False
    ):
    """
    Writes creds to a .env file
    """
    filepath = Path(os.path.join(os.getcwd(), '.env'))
    if filepath.exists() and not overwrite:
        raise FileExistsError(f'File {filepath} already exist, pass in overwrite=True to overwrite it. '
                              'Warning: this will overwrite all existing values in this .env file.')
    with open(filepath, "w") as _file:
        _file.write(creds)
    return filepath

def save_yml(
        creds: str,
        filepath: str = None,
        overwrite: bool = True
    ):
    """
    Write creds to a .yml file
    """
    if filepath is None:
        filepath = os.getcwd()
        filepath += 'credentials.yml'

    if filepath[-1] == "/":
        filepath = filepath[:-1]

    if filepath.split(".")[-1] not in ['yaml', 'yml']:
        filepath += ".yml"

    if os.path.exists(filepath):
        if overwrite:
            logger.warning(f"Overwriting existing file {filepath}")
        else:
            raise FileExistsError(f'{filepath} already exists. If you wish to overwrite it, ' \
                                   'pass overwrite=True and run this function again.')

    with open(filepath, "w") as _yaml:
        yaml.dump(data=creds, Dumper=yaml.SafeDumper, stream=_yaml)
