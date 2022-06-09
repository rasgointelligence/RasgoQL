"""
Transform rendering methods
"""
from collections import OrderedDict
import functools
import inspect
from itertools import combinations, permutations, product
from pathlib import Path
import re
import os
from typing import Any, Callable, Dict, List, Tuple, Optional, TYPE_CHECKING

import jinja2
import pandas as pd
import rasgotransforms as rtx
import yaml

from rasgoql.errors import TransformRenderingError
from rasgoql.primitives.enums import check_write_table_type
from rasgoql.utils.dbt import save_model_file

if TYPE_CHECKING:
    from rasgoql.primitives.transforms import Transform, TransformTemplate
    from rasgoql.data.base import DataWarehouse

RUN_QUERY_LIMIT = 100
JINJA_ENV = jinja2.Environment(extensions=['jinja2.ext.do', 'jinja2.ext.loopcontrols'])
MACRO_DIR = Path("rasgoql/rasgoql/macros")

__all__ = [
    "assemble_cte_chain",
    "assemble_view_chain",
    "create_dbt_files",
    "gen_udt_func_docstring",
    "gen_udt_func_signature",
    "serve_macros_as_templates",
]


class PrependingLoader(jinja2.BaseLoader):
    """
    Override the BaseLoader class to load a macro while creating our jinja env
    This macro will be called by our template

    This class allows templates to be written without macro import statments

    courtesy of http://codyaray.com/2015/05/auto-load-jinja2-macros
    """

    def __init__(self, delegate, prepend_template):
        self.delegate = delegate
        self.prepend_template = prepend_template

    def get_source(self, environment, template):
        prepend_source, _, prepend_uptodate = self.delegate.get_source(environment, self.prepend_template)
        main_source, main_filename, main_uptodate = self.delegate.get_source(environment, template)
        uptodate = lambda: prepend_uptodate() and main_uptodate()
        return prepend_source + main_source, main_filename, uptodate

    def list_templates(self):
        return self.delegate.list_templates()


# ======================
# Core rendering methods
# ======================


def assemble_cte_chain(
    transforms: List['Transform'],
    table_type: str = None,
):
    """
    Returns a nested CTE statement to render input Transforms
    """
    create_stmt, final_select = '', ''

    # Handle single transform chains
    if len(transforms) == 1:
        t = transforms[0]
        create_stmt = _set_create_statement(table_type, t.fqtn)
        final_select = generate_transform_sql(
            t.name,
            t.arguments,
            t.source_table,
            None,
            t._dw,
        )
        return create_stmt + final_select

    # Handle multi-transform chains
    t_list = []
    running_sql = None
    for t in transforms:
        t_sql = generate_transform_sql(
            t.name,
            t.arguments,
            t.source_table,
            running_sql,
            t._dw,
        )

        # Set final select & create statements from terminal transform
        if t == transforms[-1]:
            final_select = _set_final_select_statement(len(transforms), t_sql)
            create_stmt = _set_create_statement(table_type, t.fqtn)
        else:
            running_sql = _construct_running_sql(t_list, t_sql)
            t_cte_str = f'{t.output_alias} AS (\n{t_sql}\n) '
            t_list.append(t_cte_str)
    return create_stmt + 'WITH ' + ', \n'.join(t_list) + final_select


def assemble_view_chain(
    transforms: List['Transform'],
):
    """
    Returns a nested view statement to render input Transforms
    """
    view_list = []
    cte_list = []
    running_sql = None
    for t in transforms:
        t_sql = generate_transform_sql(
            t.name,
            t.arguments,
            t.source_table,
            running_sql,
            t._dw,
        )
        running_sql = _construct_running_sql(cte_list, t_sql)
        t_cte_str = f'{t.output_alias} AS (\n{t_sql}\n) '
        t_view_str = f'CREATE OR REPLACE VIEW {t.fqtn} AS {t_sql};'
        cte_list.append(t_cte_str)
        view_list.append(t_view_str)
    return '\n'.join(view_list)


def create_dbt_files(
    transforms: List['Transform'],
    schema: List[Tuple[str, str]],
    output_directory: str = None,
    file_name: str = None,
    config_args: OrderedDict = None,
    include_schema: bool = False,
) -> str:
    """
    Saves a dbt_project.yml and model.sql files to a directory
    """
    output_directory = output_directory or os.getcwd()
    file_name = file_name or f'{transforms[-1].output_alias}.sql'
    return save_model_file(
        sql_definition=assemble_cte_chain(transforms),
        output_directory=output_directory,
        file_name=file_name,
        config_args=config_args,
        include_schema=include_schema,
        schema=schema,
    )


def generate_transform_sql(
    name: str,
    arguments: OrderedDict,
    source_table: str = None,
    running_sql: str = None,
    dw: 'DataWarehouse' = None,
) -> str:
    """
    Returns the SQL for a Transform with applied arguments
    """
    # templates = rtx.serve_rasgo_transform_templates(dw.dw_type)
    templates = serve_macros_as_templates(dw.dw_type)
    udt: 'TransformTemplate' = [t for t in templates if t.name == name][0]
    if not udt:
        raise TransformRenderingError(f'Cannot find a transform named {name}')
    try:
        source_code = udt.source_code
        if udt.name == 'apply':
            source_code = arguments.pop('sql')
            if not source_code:
                raise TransformRenderingError(
                    f'Custom transform {udt.name} must provide the "sql" argument with template to apply'
                )
        # return render_template(name, source_code, arguments, source_table, running_sql, dw)
        return render_template_from_macro(name, source_code, arguments, source_table, running_sql, dw)
    except Exception as e:
        raise TransformRenderingError(e) from e


def get_macro_names() -> List[Dict[str, Any]]:
    """
    Returns all available macros from the yml file
    Includes: name, arguments
    """
    with open(MACRO_DIR / "macros.yml", "r") as _yml:
        contents = yaml.safe_load(_yml)
    return [macro for macro in contents.get("macros")]


def render_template(
    name: str,
    source_code: str,
    arguments: OrderedDict,
    source_table: str,
    running_sql: str,
    dw: 'DataWarehouse',
) -> str:
    """
    Returns a SQL statement generated by applying arguments to a Jinja template
    """
    template_fns = _source_code_functions(dw, source_table, running_sql)
    template = JINJA_ENV.from_string(source_code)
    rendered = template.render(**arguments, **template_fns)
    if not rendered:
        raise TransformRenderingError(
            f'Rendering of transform {name} produced an empty string. Please check template arguments and try again.'
        )
    return rendered


def render_template_from_macro(
    template_name: str,
    macro_code: str,
    arguments: OrderedDict,
    source_table: str,
    running_sql: str,
    dw: 'DataWarehouse',
) -> str:
    """
    Returns a SQL statement generated by applying arguments to a Jinja template
    """
    template_fns = _source_code_functions(dw, source_table, running_sql)
    arguments_list = [f"{key}={key}" for key in arguments.keys()]
    template_code = "{{" + template_name + f"({', '.join(arguments_list)})" + "}}"
    base_loader = jinja2.DictLoader({template_name: template_code, 'macro': macro_code})
    loader = PrependingLoader(base_loader, 'macro')
    env = jinja2.Environment(loader=loader, extensions=['jinja2.ext.do', 'jinja2.ext.loopcontrols'])
    template = env.get_template(template_name)
    rendered = template.render(**arguments, **template_fns)
    if not rendered:
        raise TransformRenderingError(
            f'Rendering of transform {template_name} produced an empty string. Please check template arguments and try again.'
        )
    return rendered


def serve_macros_as_templates(dw_type: str) -> List['TransformTemplate']:
    """
    Returns all available macros in a List of TransformTemplates
    Includes: name, arguments, source code
    """
    from rasgoql.primitives.transforms import TransformTemplate

    template_list = []
    macros_list = get_macro_names()
    for macro in macros_list:
        macro_name = macro.get("name")
        # TODO: Route by dw_type
        macro_sql = Path(MACRO_DIR / f"{macro_name}.sql")
        with open(macro_sql) as _sql:
            contents = _sql.read()
        template_list.append(
            TransformTemplate(
                name=macro_name,
                source_code=contents,
                arguments=macro.get("arguments"),
                description=macro.get('description'),
                tags=macro.get('tags'),
            )
        )
    return template_list


# ======================
# Helper Functions
# ======================


def _collapse_cte(
    sql: str,
) -> str:
    """
    Returns a collapsed CTE if sql is a CTE, or original sql
    """
    if sql.upper().startswith('WITH'):
        return re.sub(r'^(WITH)\s', r', ', sql, 1, flags=re.IGNORECASE)
    return sql


def _construct_running_sql(
    cte_list: List[str],
    sql: str,
) -> str:
    """
    Constructs and returns a running sql statement
    """
    # running_sql tracks the CTE chain up to this transform,
    # this is consumed by the next transform if it needs to
    # access the chain's data mid-state (e.g. confirming columns exits)
    if len(cte_list) == 0:
        return sql
    return 'WITH ' + ', \n'.join(cte_list) + _collapse_cte(sql)


def _set_create_statement(
    table_type: str,
    fqtn: str,
) -> str:
    """
    Returns a create statement or a blank string
    """
    if table_type:
        table_type = check_write_table_type(table_type)
        return f'CREATE OR REPLACE {table_type} {fqtn} AS \n'
    return ''


def _set_final_select_statement(
    transform_count: int,
    sql: str,
) -> str:
    """
    Returns a create statement or a blank string
    """
    if transform_count > 1:
        return _collapse_cte(sql)
    return sql


def gen_udt_func_docstring(
    transform: 'TransformTemplate',
) -> str:
    """
    Generate and return a docstring for a transform func
    with transform description, args, and return specified.
    """
    docstring = f"\n{transform.description}"

    docstring = f"{docstring}\n  Args:"
    for t_arg in transform.arguments:
        docstring = f"{docstring}\n    {t_arg.get('name')}: {t_arg.get('description')}"
    docstring = f"{docstring}\n    operation_name: Name to set for the operation"

    return (
        f"{docstring}\n\n  Returns:\n    Returns an new dataset with the referenced "
        f"{transform.name!r} added to this dataset's definition"
    )


def gen_udt_func_signature(
    udt_func: Callable,
    transform: 'TransformTemplate',
) -> inspect.Signature:
    """
    Creates and returns a UDT param signature.

    This is shown documentation for the parameters when hitting shift tab in a notebook
    """
    sig = inspect.signature(udt_func)

    udt_params = []
    for t_arg in transform.arguments:
        p = inspect.Parameter(name=t_arg.get('name'), kind=inspect.Parameter.KEYWORD_ONLY)
        udt_params.append(p)

    op_name_param = inspect.Parameter(
        name='operation_name',
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=None,
    )
    udt_params.append(op_name_param)
    return sig.replace(parameters=udt_params)


# ======================
# Source Code Functions
# ======================


def _cleanse_template_symbol(
    symbol: str,
) -> str:
    """
    Extra verbose function for clarity

    remove double quotes
    replace spaces and dashes with underscores
    cast to upper case
    delete anything that is not letters, numbers, or underscores
    if first character is a number, add an underscore to the beginning
    """
    symbol = str(symbol)
    symbol = symbol.strip()
    symbol = symbol.replace(' ', '_').replace('-', '_')
    symbol = symbol.upper()
    symbol = re.sub('[^A-Z0-9_]+', '', symbol)
    symbol = '_' + symbol if symbol[0].isdecimal() or not symbol else symbol
    return symbol


def _get_columns(
    source_table: str,
    running_sql: str = None,
    dw: 'DataWarehouse' = None,
) -> Dict[str, str]:
    """
    Return the column names of a given table (or sql statement)
    """
    return {
        row[0]: row[1]
        for row in dw.get_schema(
            fqtn=dw.magic_fqtn_handler(source_table, dw.default_namespace),
            create_sql=running_sql,
        )
    }


def _raise_exception(
    message: str,
) -> None:
    """
    Raise an exception to return to users of a template
    """
    raise TransformRenderingError(message)


def _run_query(
    query: str,
    source_table: str = None,
    running_sql: str = None,
    dw: 'DataWarehouse' = None,
) -> pd.DataFrame:
    """
    Jinja Func to materialize a chain as a temporary view before running a query
    """
    try:
        if running_sql:
            create_sql = f"CREATE OR REPLACE VIEW {source_table} AS {running_sql} LIMIT {RUN_QUERY_LIMIT}"
            dw.execute_query(create_sql, response='none', acknowledge_risk=True)
        return dw.execute_query(query, response='df', acknowledge_risk=True)
    except Exception as e:
        raise
    finally:
        if running_sql:
            drop_sql = f"DROP VIEW IF EXISTS {source_table}"
            dw.execute_query(drop_sql, response='none', acknowledge_risk=True)


def _source_code_functions(
    dw: 'DataWarehouse',
    source_table: str = None,
    running_sql: str = None,
) -> Dict[str, Callable]:
    """
    Get custom functions to add to the template parser
    """
    return {
        "run_query": functools.partial(
            _run_query,
            dw=dw,
            source_table=source_table,
            running_sql=running_sql,
        ),
        "cleanse_name": _cleanse_template_symbol,
        "raise_exception": _raise_exception,
        "get_columns": functools.partial(_get_columns, dw=dw, running_sql=running_sql),
        "itertools": {
            "combinations": combinations,
            "permutations": permutations,
            "product": product,
        },
    }
