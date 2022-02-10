"""
Transform rendering methods
"""
import functools
import inspect
import logging
import re
from itertools import combinations, permutations, product
from typing import Callable, Dict, List, Optional

import jinja2
import pandas as pd
from rasgotransforms import serve_rasgo_transform_templates

from rasgoql.errors import TransformRenderingError
from .enums import check_table_type

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

RUN_QUERY_LIMIT = 100
JINJA_ENV = jinja2.Environment(extensions=['jinja2.ext.do', 'jinja2.ext.loopcontrols'])


def assemble_cte_chain(
        transforms: List['Transform'],
        table_type: str = None
    ):
    """
    Returns a nested CTE statement to render input Transforms
    """
    if table_type:
        table_type = check_table_type(table_type)

    # Handle single transform chains
    if len(transforms) == 1:
        t = transforms[0]
        if table_type in ['TABLE', 'VIEW']:
            create_stmt = f'CREATE OR REPLACE {table_type} {t.fqtn} AS \n'
        else:
            create_stmt = ''
        final_select = generate_transform_sql(
            t.name,
            t.arguments,
            t.source_table,
            None,
            t._dw
        )
        return create_stmt + final_select

    # Handle multi-transform chains
    t_list = []
    running_sql = None
    i = 1
    for t in transforms:
        logger.debug(f'Rendering transform {t.name} ({i} of {len(transforms)})')
        i += 1

        # Set final select & create statements from terminal transform
        if t == transforms[-1]:
            final_select = generate_transform_sql(
                t.name,
                t.arguments,
                t.source_table,
                running_sql,
                t._dw
            )
            if table_type in ['TABLE', 'VIEW']:
                create_stmt = f'CREATE OR REPLACE {table_type} {t.fqtn} AS \n'
            else:
                create_stmt = ''
        else:
            t_sql = generate_transform_sql(
                t.name,
                t.arguments,
                t.source_table,
                running_sql,
                t._dw
            )
            # running_sql tracks the CTE chain up to this transform,
            # this is consumed by the next transform if it needs to
            # access the chain's data mid-state (e.g. confirming columns exits)
            if len(t_list) == 0:
                running_sql = t_sql
            else:
                running_sql = 'WITH ' + ', \n'.join(t_list) + t_sql
            t_cte_str = f'{t.output_alias} AS (\n{t_sql}\n) '
            t_list.append(t_cte_str)
    return create_stmt + 'WITH ' + ', \n'.join(t_list) + final_select

def assemble_view_chain(
        transforms: List['Transform']
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
            t._dw
            )
        if len(cte_list) == 0:
            running_sql = t_sql
        else:
            running_sql = 'WITH ' + ', \n'.join(cte_list) + t_sql
        t_cte_str = f'{t.output_alias} AS (\n{t_sql}\n) '
        t_view_str = f'CREATE OR REPLACE VIEW {t.fqtn} AS {t_sql};'
        cte_list.append(t_cte_str)
        view_list.append(t_view_str)
    return '\n'.join(view_list)

def generate_transform_sql(
        name: str,
        arguments: dict,
        source_table: str = None,
        running_sql: str = None,
        dw: 'DataWarehouse' = None
    ) -> str:
    """
    Returns the SQL for a Transform with applied arguments
    """
    templates = serve_rasgo_transform_templates()
    udt: 'TransformTemplate' = [t for t in templates if t.name == name][0]
    if not udt:
        raise TransformRenderingError(f'Cannot find a transform named {name}')
    try:
        source_code = udt.source_code
        if udt.name == 'apply':
            source_code = arguments.pop('sql')
            if not source_code:
                raise TransformRenderingError(f'Custom transform {udt.name} must provide ' \
                                               'the "sql" argument with template to apply')
        return render_template(name, source_code, arguments, source_table, running_sql, dw)
    except Exception as e:
        raise TransformRenderingError(e)

def render_template(
        name: str,
        source_code: str,
        arguments: dict,
        source_table: str,
        running_sql: str,
        dw: 'DataWarehouse'
    ) -> str:
    """
    Returns a SQL statement generated by applying arguments to a Jinja template
    """
    template_fns = _source_code_functions(dw, source_table, running_sql)
    template = JINJA_ENV.from_string(source_code)
    rendered = template.render(**arguments, **template_fns)
    if not rendered:
        raise TransformRenderingError(f'Rendering of transform {name} produced an empty string. ' \
                                       'Please check template arguments and try again.')
    return rendered

def _cleanse_template_symbol(
        symbol: str
    ) -> str:
    """
    Extra verbose function for clarity

    remove double quotes
    replace spaces and dashes with underscores
    cast to upper case
    delete anything that is not letters, numbers, or underscores
    if first character is a number, add an underscore to the beginning
    """
    symbol = symbol.strip()
    symbol = symbol.replace(' ', '_').replace('-', '_')
    symbol = symbol.upper()
    symbol = re.sub('[^A-Z0-9_]+', '', symbol)
    symbol = '_'+symbol if symbol[0].isdecimal() or not symbol else symbol
    return symbol

def _raise_exception(
        message: str
    ) -> None:
    """
    Raise an exception to return to users of a template
    """
    raise TransformRenderingError(message)

def _run_query(
        query: str,
        source_table: str = None,
        running_sql: str = None,
        dw: 'DataWarehouse' = None
    ) -> pd.DataFrame:
    """
    Jinja Func to materialize a chain as a temporary view before running a query
    """
    try:
        if running_sql > '':
            create_sql = f"CREATE OR REPLACE VIEW {source_table} AS {running_sql} LIMIT {RUN_QUERY_LIMIT}"
            dw.execute_query(create_sql, response='none', acknowledge_risk=True)
        return dw.execute_query(query, response='df', acknowledge_risk=True)
    except Exception as e:
        raise TransformRenderingError(e)
    finally:
        if running_sql:
            drop_sql = f"DROP VIEW IF EXISTS {source_table}"
            dw.execute_query(drop_sql, response='none', acknowledge_risk=True)

def _source_code_functions(
        dw: 'DataWarehouse',
        source_table: str = None,
        running_sql: str = None
    ) -> Dict[str, Callable]:
    """
    Get custom functions to add to the template parser
    """
    return {
        "run_query": functools.partial(_run_query, dw=dw, source_table=source_table, running_sql=running_sql),
        "cleanse_name": _cleanse_template_symbol,
        "raise_exception": _raise_exception,
        "itertools": {
            "combinations": combinations,
            "permutations": permutations,
            "product": product
        }
    }

def _gen_udt_func_signature(
        udt_func: Callable,
        transform: 'TransformTemplate'
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
        default=None
    )
    udt_params.append(op_name_param)
    return sig.replace(parameters=udt_params)


def _gen_udt_func_docstring(
        transform: 'TransformTemplate'
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

    docstring = f"{docstring}\n\n  Returns:\n    Returns an new dataset with the referenced " \
                f"{transform.name!r} added to this dataset's definition"
    return docstring
