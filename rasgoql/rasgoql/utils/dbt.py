"""
Holds dbt helpers
"""
import logging
import os
import string
from typing import List, Tuple

from pathlib import Path

import yaml

from rasgoql.utils.sql import parse_namespace

logging.basicConfig()
logger = logging.getLogger('dbt')
logger.setLevel(logging.INFO)

DBT_MODEL_CONFIG_TEMPLATE = '''
  config(
    {config_args}
  )
'''

DBT_PROJECT_TEMPLATE = {
    "name": "",
    "version": "1.0.0",
    "config-version": 2,
    "profile": "default",
    "model-paths": ["models"],
    "analysis-paths": ["analyses"],
    "test-paths": ["tests"],
    "seed-paths": ["seeds"],
    "macro-paths": ["macros"],
    "snapshot-paths": ["snapshots"],
    "target-path": "target",
    "log-path": "logs",
    "packages-install-path": "dbt_packages",
    "clean-targets": ["target", "dbt_packages"],
    "models": None
}


def check_project_name(
        project_name: str
    ):
    """
    Checks a project name for dbt compliance
    """
    allowed = set(string.ascii_lowercase + '_')
    if any(char for char in project_name.lower() if char not in allowed):
        logger.warning(
            "per dbt: Project names should contain only lowercase characters "
            "and underscores. A good package name should reflect your organization's "
            "name or the intended use of these models"
        )
    return project_name.lower()

def prepare_dbt_path(
        project_name: str,
        project_directory: Path
    ) -> None:
    """
    Checks for a specified filepath and creates one if it doesn't exist
    """
    dbt_dirs = ['analyses', 'dbt_packages', 'logs', 'macros',
                'models', 'seeds', 'target', 'tests']
    if project_name not in project_directory:
        project_directory = os.path.join(project_directory, project_name)
    if not os.path.exists(project_directory):
        os.makedirs(project_directory)
    for dir_name in dbt_dirs:
        this_dir = os.path.join(project_directory, dir_name)
        if not os.path.exists(this_dir):
            os.makedirs(this_dir)
    return project_directory

def save_project_file(
        project_name: str,
        filepath: Path,
        namespace: str,
        materialize: str
    ) -> bool:
    """
    Writes a yaml definition to a dbt project file
    """
    if not os.path.exists(filepath):
        yml_definition = DBT_PROJECT_TEMPLATE.copy()
        yml_definition["name"] = project_name
        model_config = {project_name: {"+materialize": materialize}}
        if namespace:
            db, schema = parse_namespace(namespace)
            model_config[project_name].update({"database": db})
            model_config[project_name].update({"schema": schema})
        yml_definition["models"] = model_config
        with open(filepath, "w") as _yaml:
            yaml.dump(data=yml_definition, Dumper=yaml.SafeDumper, stream=_yaml, sort_keys=False)
    return filepath

def save_model_file(
        sql_definition: str,
        output_directory: Path,
        file_name: str,
        schema: List[Tuple[str, str]],
        config_args: dict = None,
        include_schema: bool = False
) -> bool:
    """
    Writes a sql script to a dbt model file
    """
    filepath = os.path.join(output_directory, file_name)
    if config_args:
        # TODO: Should we validate whether args are valid dbt keywords?
        model_config = DBT_MODEL_CONFIG_TEMPLATE.format(
            config_args=config_args
        )
        model_config = '{{' + model_config + '}}'
        sql_definition = f'{model_config}\n\n{sql_definition}'
    with open(filepath, "w") as _file:
        _file.write(sql_definition)
    if include_schema:
        model_name = file_name.replace('.sql', '')
        save_schema_file(output_directory, model_name, schema, config_args)
    return filepath

def save_schema_file(
        output_directory: Path,
        model_name: str,
        schema: List[Tuple[str, str]],
        config_args: dict = None,
    ):
    """
    Writes a table def to a dbt schema file
    """
    filepath = os.path.join(output_directory, 'schema.yml')
    schema_definition = None
    columns_list = []
    for row in schema:
        columns_list.append({"name:": row[0]})
    model_dict = {"name": model_name, "columns": columns_list}
    if config_args:
        model_dict.update({"config": config_args})
    if not os.path.exists(filepath):
        schema_definition = {"version": 2, "models": [model_dict]}
    else:
        schema_definition = [model_dict]
    with open(filepath, "a") as _file:
        yaml.dump(data=schema_definition, Dumper=yaml.SafeDumper, stream=_file, sort_keys=False)
    return filepath
