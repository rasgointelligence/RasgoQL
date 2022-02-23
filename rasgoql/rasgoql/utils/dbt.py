"""
Holds dbt helpers
"""
import logging
import os
import string

from pathlib import Path

import yaml

from rasgoql.utils.sql import parse_namespace

logging.basicConfig()
logger = logging.getLogger('dbt')
logger.setLevel(logging.INFO)

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
    "models": {}
}

MODEL_CONFIG_TEMPLATE = '''
  config(
    {materialize_config}
    {database_config}
    {schema_config}
  )
'''


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
        filepath: Path,
        namespace: str = None,
        materialize: str = 'view'
) -> bool:
    """
    Writes a sql script to a dbt model file
    """
    materialize_config = f"materialize = '{materialize}'"
    database_config = ""
    schema_config = ""
    if namespace:
        db, schema = parse_namespace(namespace)
        database_config = f",database = '{db}'"
        schema_config = f",schema = '{schema}'"
    model_config = MODEL_CONFIG_TEMPLATE.format(
        materialize_config=materialize_config,
        database_config=database_config,
        schema_config=schema_config
    )
    model_config = '{{' + model_config + '}}'
    sql_definition = f'{model_config}\n\n{sql_definition}'
    with open(filepath, "w") as _sql:
        _sql.write(sql_definition)
    return filepath
