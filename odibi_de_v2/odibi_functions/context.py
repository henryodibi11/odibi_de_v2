from typing import Optional, Any
from threading import local


_thread_local = local()


class ExecutionContext:
    """Container for runtime execution context.
    
    Holds runtime information like spark session, configuration, logger,
    and other resources that functions may need during execution.
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import ExecutionContext
        >>> 
        >>> # Create context with spark session
        >>> context = ExecutionContext(spark=spark_session, config=my_config)
        >>> 
        >>> # Access context attributes
        >>> df = context.spark.table("my_table")
        >>> log_level = context.config.get("log_level")
    """
    
    def __init__(self, **kwargs):
        """Initialize context with arbitrary attributes.
        
        Args:
            **kwargs: Context attributes (spark, config, logger, etc.)
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def __repr__(self) -> str:
        attrs = {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
        return f"ExecutionContext({attrs})"


def set_current_context(context: Optional[ExecutionContext]) -> None:
    """Set the current thread-local execution context.
    
    Args:
        context: ExecutionContext instance or None to clear
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import set_current_context, ExecutionContext
        >>> 
        >>> context = ExecutionContext(spark=spark_session)
        >>> set_current_context(context)
    """
    _thread_local.context = context


def get_current_context() -> Optional[ExecutionContext]:
    """Get the current thread-local execution context.
    
    Returns:
        Current ExecutionContext or None if not set
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import get_current_context
        >>> 
        >>> context = get_current_context()
        >>> if context and hasattr(context, 'spark'):
        ...     df = context.spark.table("my_table")
    """
    return getattr(_thread_local, 'context', None)


def clear_context() -> None:
    """Clear the current thread-local execution context.
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import clear_context
        >>> clear_context()
    """
    _thread_local.context = None
