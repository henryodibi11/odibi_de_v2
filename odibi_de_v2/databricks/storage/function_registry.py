import importlib
import pkgutil
import inspect
from typing import Callable, Dict, Optional

# Default and override paths
_DEFAULT_REGISTRY_PACKAGE = "odibi_de_v2.databricks.storage"
_USER_DEFINED_PACKAGE: Optional[str] = None
_FUNCTION_REGISTRY: Optional[Dict[str, Callable]] = None


def set_registry_package(package_name: str):
    """
    Sets the package used for searching save functions, overriding the default.

    This function updates the global registry to use a user-specified package for locating save functions, effectively
    replacing any previously set custom package. It also clears any cached functions to ensure that the new package
    settings are used on subsequent operations.

    Args:
        package_name (str): The dotted import path to the user-defined package where save functions are located.

    Returns:
        None

    Example:
        >>> set_registry_package("my_project.custom_savers")

    Note:
        This function modifies global state and should be used with caution, especially in multi-threaded environments.
    """
    global _USER_DEFINED_PACKAGE, _FUNCTION_REGISTRY
    _USER_DEFINED_PACKAGE = package_name
    _FUNCTION_REGISTRY = None  # Clear cache


def get_function_registry() -> Dict[str, Callable]:
    """
    Retrieves a cached dictionary of function names mapped to their callable references.

    This function checks if a global `_FUNCTION_REGISTRY` is already defined. If not, it initializes this registry by
    discovering save functions from either a user-defined package or a default package. The registry is then cached
    globally for future use.

    Returns:
        Dict[str, Callable]: A dictionary where keys are function names and values are the corresponding
        callable objects.

    Raises:
        ImportError: If the function discovery fails due to missing packages or incorrect setup.

    Example:
        >>> registry = get_function_registry()
        >>> registry["save_or_merge_delta"](df, config, spark, connector)
    """
    global _FUNCTION_REGISTRY
    if _FUNCTION_REGISTRY is None:
        package = _USER_DEFINED_PACKAGE or _DEFAULT_REGISTRY_PACKAGE
        _FUNCTION_REGISTRY = discover_save_functions(package)
    return _FUNCTION_REGISTRY


def discover_save_functions(
    package_name: str = _DEFAULT_REGISTRY_PACKAGE
        ) -> Dict[str, Callable]:
    """
    Discovers and registers functions prefixed with 'save_' or 'merge_' from a specified package.

    This function scans a given Python package for any functions whose names start with 'save_' or 'merge_'.
    It dynamically imports these functions and compiles them into a dictionary, facilitating easy access and
    use elsewhere in your application.

    Args:
        package_name (str): The name of the package from which to discover functions. Defaults to a predefined package
            name stored in `_DEFAULT_REGISTRY_PACKAGE`.

    Returns:
        Dict[str, Callable]: A dictionary where keys are the names of the discovered functions and values are the
            callable functions themselves.

    Raises:
        ImportError: If the specified package cannot be imported.
        AttributeError: If the package does not have a `__path__` attribute, indicating it is not a package.

    Example:
        >>> discovered_functions = discover_save_functions('my_app.data_handlers')
        >>> discovered_functions['save_user_data'](user_data)
    """
    registry: Dict[str, Callable] = {}
    package = importlib.import_module(package_name)

    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        module = importlib.import_module(f"{package_name}.{module_name}")
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if name.startswith(("save_", "merge_")):
                registry[name] = obj

    return registry