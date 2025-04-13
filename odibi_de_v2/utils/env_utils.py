"""
env_utils.py

Utility functions for detecting execution environment and accessing
environment variables safely. Enables adaptive behavior in pipelines.
"""

import os
import sys


def is_running_in_databricks() -> bool:
    """
    Detects whether the current code is running in a Databricks environment.

    Returns:
        bool: True if running in Databricks, False otherwise.

    Example:
        >>> is_running_in_databricks()
        True
    """
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DATABRICKS_HOST" in os.environ
        or "databricks-connect" in sys.executable.lower()
    )


def is_running_in_notebook() -> bool:
    """
    Detects whether the code is running in a notebook environment
        (Jupyter or IPython).

    Returns:
        bool: True if running in a notebook, False otherwise.

    Example:
        >>> is_running_in_notebook()
        True
    """
    try:
        from IPython import get_ipython
        shell = get_ipython().__class__.__name__
        return shell in ("ZMQInteractiveShell", "Shell")
    except Exception:
        return False


def is_local_env() -> bool:
    """
    Checks if the current environment is local
        (i.e., not in Databricks or cloud).

    Returns:
        bool: True if running locally, False otherwise.

    Example:
        >>> is_local_env()
        True
    """
    return not is_running_in_databricks()


def get_current_env() -> str:
    """
    Returns a string label describing the current environment.

    Returns:
        str: "databricks", "local", or "unknown"

    Example:
        >>> get_current_env()
        'local'
    """
    if is_running_in_databricks():
        return "databricks"
    elif is_running_in_notebook() or is_local_env():
        return "local"
    else:
        return "unknown"


def get_env_variable(key: str, default: str = None) -> str:
    """
    Safely retrieves an environment variable, with optional default fallback.

    Args:
        key (str): Environment variable key.
        default (str): Value to return if key is not set.

    Returns:
        str: The environment variable value or the default.

    Example:
        >>> get_env_variable("STORAGE_ACCOUNT", "default_account")
        'default_account'
    """
    return os.getenv(key, default)
