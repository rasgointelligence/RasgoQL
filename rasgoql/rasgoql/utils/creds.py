"""
DataWarehouse credential functions
"""
import os
import yaml

import dotenv


def load_env(
        filepath: os.PathLike = None
    ):
    """
    Loads a .env file, allowing contents to be callable by os.getenv()
    """
    if not filepath:
        filepath = os.getcwd()
    if not filepath.endswith('.env'):
        filepath = os.path.join(filepath, '.env')
    if not os.path.exists(filepath):
        raise FileNotFoundError(f'File {filepath} does not exist')
    dotenv.load_dotenv(filepath)

def load_yml(
        filepath: os.PathLike
    ):
    """
    Loads a yml file and returns the contents
    """
    with open(filepath) as yml_file:
        contents = yaml.load(yml_file, Loader=yaml.Loader)
    return contents

def read_file(
        filepath: os.PathLike = None
    ) -> str:
    """
    Reads the contents of a file
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f'File {filepath} does not exist')
    with open(filepath, "r") as _file:
        contents = _file.read()
    return contents

def save_env(
        creds: dict,
        filepath: os.PathLike = None,
        overwrite: bool = False
    ):
    """
    Writes creds to a .env file
    """
    if not filepath:
        filepath = os.getcwd()
    if not filepath.endswith('.env'):
        filepath = os.path.join(filepath, '.env')
    if os.path.exists(filepath):
        if not overwrite:
            raise FileExistsError(
                f'File {filepath} already exist, pass in overwrite=True to overwrite it. '
                'Warning: this will overwrite all existing values in this .env file.')
    else:
        f = open(filepath, "x")
        f.close()
    for k, v in creds.items():
        dotenv.set_key(filepath, k, v)
    return filepath

def save_yml(
        creds: str,
        filepath: os.PathLike = None,
        overwrite: bool = True
    ):
    """
    Write creds to a .yml file
    """
    if filepath is None:
        filepath = os.getcwd()

    if filepath.endswith('/'):
        filepath = os.path.join(filepath, 'credentials.yml')

    if filepath.split(".")[-1] not in ['yaml', 'yml']:
        filepath = os.path.join(filepath, ".yml")

    if os.path.exists(filepath) and not overwrite:
        raise FileExistsError(
            f'{filepath} already exists. If you wish to overwrite it, '
            'pass overwrite=True and run this function again.')
    with open(filepath, "w") as _yaml:
        yaml.dump(data=creds, Dumper=yaml.SafeDumper, stream=_yaml)
    return filepath
