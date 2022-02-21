"""
Holds dbt helpers
"""
import os
from pathlib import Path
import yaml

DBT_PROKECT_TEMPLATE = {
    "name": "",
    "version": "1.0.0",
    "profile": "",
    "source-paths": ["models"],
    "target-path": "target",
    "models": {}
}

def prepare_dbt_path(
        project_name: str,
        filepath: Path
    ) -> None:
    """
    Checks for a specified filepath and creates one if it doesn't exist
    """
    project_dir = os.path.join(filepath, project_name)
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)
    models_dir = os.path.join(project_dir, "models")
    if not os.path.exists(models_dir):
        os.makedirs(models_dir)

def save_project_file(
        yml_definition: dict,
        filepath: Path,
        overwrite: bool = True
    ) -> bool:
    """
    Writes a yaml definition to a dbt project file
    """
    if os.path.exists(filepath) and not overwrite:
        raise FileExistsError(
            f'{filepath} already exists. If you wish to overwrite it, '
            'pass overwrite=True and run this function again.')
    with open(filepath, "w") as _yaml:
        yaml.dump(data=yml_definition, Dumper=yaml.SafeDumper, stream=_yaml)
    return filepath

def save_model_file(
        sql_definition: str,
        filepath: Path,
        overwrite: bool = True
) -> bool:
    """
    Writes a sql script to a dbt model file
    """
    if os.path.exists(filepath) and not overwrite:
        raise FileExistsError(
            f'{filepath} already exists. If you wish to overwrite it, '
            'pass overwrite=True and run this function again.')
    with open(filepath, "w") as _sql:
        _sql.write(sql_definition)
    return filepath
