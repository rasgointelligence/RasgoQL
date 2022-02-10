"""
Decorators
"""
import functools
from typing import Callable, Union


def parametrized(decorator):
    """
    Meta-decorator to support passing parameters into future decorators
    """
    @functools.wraps(decorator)
    def decorator_maker(*args, **kwargs):
        def decorator_wrapper(func):
            return decorator(func, *args, **kwargs)
        return decorator_wrapper
    return decorator_maker

def only_rasgo_tables(func: Callable) -> Callable:
    """
    Decorator to restrict Dataset methods
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: 'Dataset' = args[0]
        if not self.is_rasgo:
            raise NotImplementedError(f'{func.__name__} method is only available for Rasgo-created tables and views')
        return func(*args, **kwargs)
    return wrapper

def require_dw(func: Callable) -> Callable:
    """
    Decorator to restrict Class methods
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: Union['Dataset', 'Transform', 'SQLChain'] = args[0]
        if not self._dw:
            raise NotImplementedError(f'{func.__name__} method is only available for classes instantiated with a DW connection')
        return func(*args, **kwargs)
    return wrapper

def require_transforms(func: Callable) -> Callable:
    """
    Decorator to restrict Class methods
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: 'SQLChain' = args[0]
        if not self.transforms:
            raise NotImplementedError(f'{func.__name__} method is only available for SQLChains with at least one Transform')
        return func(*args, **kwargs)
    return wrapper

@parametrized
def restrict_table_type(func: Callable, table_type: str) -> Callable:
    """
    Decorator to restrict Dataset methods
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: 'Dataset' = args[0]
        if self.table_type != table_type.upper():
            raise NotImplementedError(f'{func.__name__} method is only available for {table_type.upper()}s')
        return func(*args, **kwargs)
    return wrapper
