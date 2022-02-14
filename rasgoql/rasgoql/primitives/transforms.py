"""
Primitive Classes
"""
import logging
from typing import Callable, List

import pandas as pd
import rasgotransforms as rtx

from rasgoql.errors import ParameterException
from rasgoql.utils.decorators import require_dw, require_transforms
from rasgoql.utils.sql import parse_fqtn, random_table_name, validate_fqtn

from .enums import (
    check_render_method, check_table_type,
    TableType, TableState
)
from .rendering import (
    assemble_cte_chain, assemble_view_chain,
    _gen_udt_func_docstring, _gen_udt_func_signature
)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
        for transform in rtx.serve_rasgo_transform_templates():
            f = self._create_aliased_function(transform)
            setattr(self, transform.name, f)

    @require_dw
    def transform(
            self,
            name: str = None,
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
        # If we're transforming a Chain
        if isinstance(self, SQLChain):
            entry_table = self.entry_table
            source_table = self.output_table.fqtn
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
            self.namespace,
            output_alias,
            self._dw
        )
        transforms.append(new_transform)
        return SQLChain(entry_table, transforms, self._dw)


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
        if not validate_fqtn(fqtn):
            raise ParameterException("Must pass in a valid 'fqtn' parameter to create a Dataset")
        self.fqtn = fqtn
        database, schema, table_name = parse_fqtn(fqtn)
        self.namespace = f'{database}.{schema}'
        self.table_name = table_name
        self._dw._validate_namespace(self.namespace)

        self.table_type = TableType.UNKNOWN
        self.table_state = TableState.UNKNOWN
        self.is_rasgo = False
        self._dw_sync()

    def __repr__(self) -> str:
        return f"Dataset(fqtn={self.fqtn}, type={self.table_type.value})"

    @require_dw
    def _dw_sync(self):
        """
        Synchronize status with Cloud DataWarehouse
        """
        obj_exists, is_rasgo_obj, obj_type = self._dw.get_object_details(self.fqtn)
        if obj_exists:
            self.table_state = TableState.IN_DW
            self.table_type = TableType[obj_type]
            self.is_rasgo = is_rasgo_obj

    @require_dw
    def get_schema(self) -> dict:
        """
        Return the schema of this table
        """
        return self._dw.get_schema(self.fqtn)

    @require_dw
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
    def to_df(self) -> pd.DataFrame:
        """
        Return a pandas DataFrame of the entire table
        """
        return self._dw.execute_query(f"SELECT * FROM {self.fqtn}", response='df')


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
        return self.namespace + '.' + self.output_alias


class SQLChain(TransformableClass):
    """
    Reference to one or more Transforms
    """

    def __init__(
            self,
            entry_table: Dataset,
            transforms: List[Transform] = None,
            dw: 'DataWarehouse' = None
        ) -> None:
        super().__init__(dw)
        self.entry_table = entry_table
        self.transforms = transforms
        self.namespace = entry_table.namespace

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
        self._dw._validate_namespace(new_namespace)
        self.namespace = new_namespace
        logger.info(f"Namespace reset to {self.namespace}")

    @property
    def fqtn(self) -> str:
        """
        Returns the fully qualified table name the transform would create if saved
        
        NOTE: This property will be dynamic until the Chain is finally saved
        """
        return self.output_table.fqtn

    @property
    def output_table(self) -> Dataset:
        """
        Returns the Dataset this SQLChain would create if saved in current state

        NOTE: This property will be dynamic until the Chain is finally saved
        """
        if self.ternimal_transform:
            return Dataset(f"{self.ternimal_transform.fqtn}", self._dw)
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
        table_type = check_table_type(table_type)
        if table_type == 'UNKNOWN':
            raise ValueError("table_type must be 'VIEW' or 'TABLE'")
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

    def to_df(self) -> pd.DataFrame:
        """
        Returns data into a pandas DataFrame
        """
        return self._dw.execute_query(self.sql(), response='df')
