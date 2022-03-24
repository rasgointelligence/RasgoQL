"""
Primitive Classes
"""
import logging
from typing import Callable, List

import pandas as pd
import rasgotransforms as rtx

from rasgoql.errors import ParameterException
from rasgoql.utils.decorators import (
    beta, require_dw,
    require_materialized, require_transforms
)
from rasgoql.utils.sql import (
    parse_fqtn, make_namespace_from_fqtn,
    random_table_name, validate_fqtn
)
from rasgoql.primitives.enums import (
    check_render_method, check_table_type, check_write_table_type,
    TableType, TableState
)
from rasgoql.primitives.rendering import (
    assemble_cte_chain, assemble_view_chain, create_dbt_files,
    _gen_udt_func_docstring, _gen_udt_func_signature
)

logging.basicConfig()
ds_logger = logging.getLogger('Dataset')
ds_logger.setLevel(logging.INFO)
chn_logger = logging.getLogger('SQLChain')
chn_logger.setLevel(logging.INFO)


class TransformableClass:
    """
    Class to attach Rasgo transform methods to other classes
    """
    def __init__(
            self,
            dw: 'DataWarehouse'
        ):
        self._dw = dw
        self._transform_sync()

    def _create_aliased_function(
            self,
            transform: 'TransformTemplate'
        ) -> Callable:
        """
        Returns a new function to dynamically attach to a class on init
        """
        def f(*arg, **kwargs) -> 'SQLChain':
            return self.transform(name=transform.name, *arg, **kwargs)

        f.__name__ = transform.name
        f.__signature__ = _gen_udt_func_signature(f, transform)
        f.__doc__ = _gen_udt_func_docstring(transform)
        return f

    def _transform_sync(self):
        """
        Gather available transforms and create aliased functions for each
        """
        for transform in rtx.serve_rasgo_transform_templates(self._dw.dw_type):
            f = self._create_aliased_function(transform)
            setattr(self, transform.name, f)

    @require_dw
    def transform(
            self,
            name: str,
            arguments: dict = None,
            output_alias: str = None,
            **kwargs
        ) -> 'SQLChain':
        """
        Apply a Rasgo transform and return a SQLChain
        """
        source_table = ''
        transforms = []
        # If we're transforming a Dataset
        if isinstance(self, Dataset):
            entry_table = self
            source_table = self.fqtn
            namespace = self._dw.default_namespace
        # If we're transforming a Chain
        if isinstance(self, SQLChain):
            entry_table = self.entry_table
            source_table = self.output_table.fqtn
            namespace = self.namespace
            # If we're transforming a Chain with existing transforms
            if self.transforms:
                source_table = self.output_table.table_name
                transforms = transforms + self.transforms
        arguments = arguments if arguments else {}
        arguments.update(kwargs)
        arguments['source_table'] = source_table
        new_transform = Transform(
            name,
            arguments,
            namespace,
            output_alias,
            self._dw
        )
        transforms.append(new_transform)
        return SQLChain(entry_table, namespace, transforms, self._dw)


class Dataset(TransformableClass):
    """
    Reference to a table or view in the Data Warehouse
    """
    def __init__(
            self,
            fqtn: str,
            dw: 'DataWarehouse' = None
        ):
        super().__init__(dw)
        try:
            self.fqtn: str = validate_fqtn(fqtn)
            self.table_name: str = parse_fqtn(fqtn)[2]
            self.namespace: str = make_namespace_from_fqtn(fqtn)
            self._dw._validate_namespace(self.namespace)
        except ValueError:
            raise ParameterException("Must pass in a valid 'fqtn' parameter to create a Dataset")
        self.table_type: str = TableType.UNKNOWN.value
        self.table_state: str = TableState.UNKNOWN.value
        self.is_rasgo: bool = False
        self._dw_sync()

    def __repr__(self) -> str:
        return f"Dataset(fqtn={self.fqtn}, " \
               f"type={self.table_type}, " \
               f"state={self.table_state})"

    @require_dw
    def _dw_sync(self):
        """
        Synchronize status with Cloud DataWarehouse
        """
        obj_exists, is_rasgo_obj, obj_type = self._dw.get_object_details(self.fqtn)
        if obj_exists:
            self.table_state = TableState.IN_DW.value
            self.table_type = check_table_type(obj_type)
            self.is_rasgo = is_rasgo_obj
        else:
            self.table_state = TableState.IN_MEMORY.value

    @require_dw
    def get_schema(self) -> dict:
        """
        Return the schema of this table
        """
        return self._dw.get_schema(self.fqtn)

    @require_dw
    @require_materialized
    def preview(self) -> pd.DataFrame:
        """
        Return a pandas DataFrame of top 10 rows
        """
        return self._dw.preview(f'SELECT * FROM {self.fqtn}')

    @require_dw
    def sql(self) -> dict:
        """
        Return the ddl to create this table
        """
        return self._dw.get_ddl(self.fqtn)

    @require_dw
    @require_materialized
    def to_df(self, batches=False) -> pd.DataFrame:
        """
        Return a pandas DataFrame of the entire table
        """
        return self._dw.execute_query(
            f"SELECT * FROM {self.fqtn}",
            response='df',
            batches=batches
        )


class TransformTemplate:
    """
    Reference to a Rasgo transform template
    """
    def __init__(
            self,
            name: str,
            arguments: List[dict],
            source_code: str,
            transform_type: str,
            description: str = None
        ):
        self.name = name
        self.arguments = arguments
        self.source_code = source_code
        self.transform_type = transform_type
        self.description = description

    def __repr__(self) -> str:
        arg_str = ', '.join(f'{arg.get("name")}: {arg.get("type")}' for arg in self.arguments)
        return f"RasgoTemplate: {self.name}({arg_str})"

    def define(self) -> str:
        """
        Return a pretty string definition of this Transform
        """
        pretty_string = f'''{self.transform_type.title()} Transform: {self.name}
        Description: {self.description}
        Arguments: {self.arguments}
        SourceCode: {self.source_code}
        '''
        return pretty_string


class Transform:
    """
    Reference to a Transform
    """
    def __init__(
            self,
            name: str,
            arguments: dict,
            namespace: str,
            output_alias: str = None,
            dw: 'DataWarehouse' = None
        ):
        self._dw = dw

        self.arguments = arguments
        self.name = name
        self.namespace = namespace
        self.output_alias = output_alias or random_table_name()
        self.source_table = arguments.get("source_table")

    def __repr__(self) -> str:
        return f"Transform({self.output_alias}: {self.name})"

    @property
    def fqtn(self) -> str:
        """
        Returns the fully qualified table name the transform would create if saved
        """
        return f'{self.namespace}.{self.output_alias}'


class SQLChain(TransformableClass):
    """
    Reference to one or more Transforms
    """

    def __init__(
            self,
            entry_table: Dataset,
            namespace: str,
            transforms: List[Transform] = None,
            dw: 'DataWarehouse' = None
        ) -> None:
        super().__init__(dw)
        self.entry_table = entry_table
        self.transforms = transforms
        self.namespace = namespace

    def __repr__(self) -> str:
        transform_count = len(self.transforms) if self.transforms else 0
        return f"SQLChain({self.entry_table.fqtn} + {transform_count} transforms)"

    def change_namespace(
            self,
            new_namespace: str
        ):
        """
        Re-sets the class's namespace
        """
        self._dw.change_namespace(new_namespace)
        self.namespace = new_namespace
        for t in self.transforms:
            t.namespace = new_namespace

    @property
    def fqtn(self) -> str:
        """
        Returns the fully qualified table name this SQLChain would create
        if saved in current state

        NOTE: This property will be dynamic until the Chain is finally saved
        """
        return self.output_table.fqtn

    @require_dw
    def get_schema(self) -> dict:
        """
        Return the table schema this SQLChain would create if saved in current state

        NOTE: This property will be dynamic until the Chain is finally saved
        """
        return self._dw.get_schema(self.fqtn, self.sql())

    @property
    def output_table(self) -> Dataset:
        """
        Returns the Dataset this SQLChain would create if saved in current state

        NOTE: This property will be dynamic until the Chain is finally saved
        """
        if self.ternimal_transform:
            return Dataset(self.ternimal_transform.fqtn, self._dw)
        return self.entry_table

    @require_dw
    @require_transforms
    def preview(self) -> pd.DataFrame:
        """
        Returns the top 10 rows of data into a pandas DataFrame
        """
        return self._dw.preview(self.sql())

    @require_dw
    def sql(
            self,
            render_method: str = 'SELECT'
        ) -> str:
        """
        Returns the SQL to build this Transform Chain

        render_method: str: ['SELECT', 'TABLE', 'VIEW', 'VIEW CHAIN']
        """
        render_method = check_render_method(render_method)
        if not self.transforms:
            return f"SELECT * FROM {self.output_table.fqtn}"
        if render_method == 'VIEWS':
            return assemble_view_chain(self.transforms)
        return assemble_cte_chain(
            self.transforms,
            render_method if render_method in ['TABLE', 'VIEW'] else None
        )

    @require_dw
    @require_transforms
    def save(
            self,
            table_name: str = None,
            table_type: str = 'view',
            overwrite: bool = False
        ):
        """
        Materializes this Transform Chain into SQL objects
        """
        table_type = check_write_table_type(table_type)
        table_name = table_name or self.output_table.fqtn
        new_table = self._dw.create(
            self.sql(),
            table_name,
            table_type,
            overwrite
        )
        return Dataset(new_table, self._dw)

    @property
    def ternimal_transform(self) -> Transform:
        """
        Returns the last transform in this chain
        """
        if self.transforms:
            return self.transforms[-1]
        return None

    @beta
    def to_dbt(
            self,
            output_directory: str = None,
            file_name: str = None,
            config_args: dict = None,
            include_schema: bool = False
        ) -> str:
        """
        Saves a new model.sql file to your dbt models directory

        Params:
        `output_directory`: str:
            directory to save model file
            defaults to current working dir
        `file_name`: str:
            defaults to {output_alias}.sql of SQLChain
        `include_schema`: bool:
            Include a schema.yml file
        `config_args`: dict:
            key value pair of
            dbt [config values](https://docs.getdbt.com/reference/model-configs)
        """
        try:
            schema = self.get_schema()
        except:
            if include_schema:
                chn_logger.warning(
                    'Unexpected error generating the schema of this SQLChain. '
                    'Your model.sql file will be generated without a schema.yml file. '
                    'This is most likely a syntax issue in your SQLChain or existing view. '
                    'Consider running your_chn.sql() to check the syntax and/or '
                    'your_chn.save() to update the view definition in your Data Warehouse.'
                )
            schema = []
        return create_dbt_files(
            self.transforms,
            schema,
            output_directory,
            file_name,
            config_args,
            include_schema
        )

    def to_df(self, batches: bool = False) -> pd.DataFrame:
        """
        Returns data into a pandas DataFrame
        """
        return self._dw.execute_query(
            self.sql(),
            response='df',
            batches=batches
        )
