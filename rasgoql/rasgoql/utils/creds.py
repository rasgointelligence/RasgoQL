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
        file_path: str = None
    ):
    """
    Loads a .env file, allowing contents to be callable by os.getenv()
    """
    dotenv.load_dotenv(file_path)

def load_yml(
        file_path: str
    ):
    """
    Loads a yml file and returns the contents
    """
    with open(file_path) as yml_file:
        contents = yaml.load(yml_file, Loader=yaml.Loader)
    return contents

def save_env(
        creds: str,
        file_path: str = None,
        overwrite: bool = False
    ):
    """
    Writes creds to a .env file
    """
    file_path = Path(os.path.join(os.getcwd(), '.env'))
    if file_path.exists() and not overwrite:
        raise FileExistsError(f'File {file_path} already exist, pass in overwrite=True to overwrite it. '
                              'Warning: this will overwrite all existing values in this .env file.')
    with open(file_path, "w") as _file:
        _file.write(creds)
    return file_path

def save_yml(
        creds: str,
        file_path: str = None,
        overwrite: bool = True
    ):
    """
    Write creds to a .yml file
    """
    if file_path is None:
        file_path = os.getcwd()
        file_path += 'credentials.yml'

    if file_path[-1] == "/":
        file_path = file_path[:-1]

    if file_path.split(".")[-1] not in ['yaml', 'yml']:
        file_path += ".yml"

    if os.path.exists(file_path):
        if overwrite:
            logger.warning(f"Overwriting existing file {file_path}")
        else:
            raise FileExistsError(f'{file_path} already exists. If you wish to overwrite it, ' \
                                   'pass overwrite=True and run this function again.')

    with open(file_path, "w") as _yaml:
        yaml.dump(data=creds, Dumper=yaml.SafeDumper, stream=_yaml)
