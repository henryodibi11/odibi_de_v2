import importlib
import inspect
import pkgutil
from typing import Dict, Optional, Type
from odibi_de_v2.core import IDataTransformer

# Default and override settings
_DEFAULT_TRANSFORMER_PACKAGE = "odibi_de_v2.transformer"
_USER_DEFINED_TRANSFORMER_PACKAGE: Optional[str] = None
_TRANSFORMER_REGISTRY: Optional[Dict[str, Type[IDataTransformer]]] = None


def set_transformer_package(package_name: str):
    """
    Sets the package from which transformer classes are loaded, overriding the default setting.

    This function updates the global setting for the package used to discover transformer classes,
    allowing the user to specify a custom module or package. It also resets the transformer registry
    to ensure that the new package is used for subsequent operations.

    Args:
        package_name (str): The dotted path to the custom transformer module or package where transformer
        classes are defined.
    Returns:
        None

    Example:
        >>> set_transformer_package("my_project.custom_transformers")

    Note:
        This function modifies global state and may affect other parts of the application that rely on the
        transformer registry.
    """
    global _USER_DEFINED_TRANSFORMER_PACKAGE, _TRANSFORMER_REGISTRY
    _USER_DEFINED_TRANSFORMER_PACKAGE = package_name
    _TRANSFORMER_REGISTRY = None


def get_transformer_registry() -> Dict[str, Type[IDataTransformer]]:
    """
    Retrieves a singleton dictionary of data transformer classes, initializing it if necessary.

    This function checks if a global transformer registry is already initialized. If not, it initializes the registry
    by discovering transformer classes within a specified package. The package can be either user-defined or a default
    one. The function ensures that all transformer classes are accessible via their class names as keys in the returned
    dictionary.

    Returns:
        Dict[str, Type[IDataTransformer]]: A dictionary mapping class names to their corresponding transformer classes,
        facilitating dynamic access and instantiation of transformers.

    Raises:
        ImportError: If the transformers' package specified cannot be imported, indicating potential issues with the
        package's availability or correctness.

    Example:
        >>> transformers = get_transformer_registry()
        >>> print(transformers['SimpleTransformer'])
        <class 'transformers.simple_transformer.SimpleTransformer'>
    """
    global _TRANSFORMER_REGISTRY
    if _TRANSFORMER_REGISTRY is None:
        package = _USER_DEFINED_TRANSFORMER_PACKAGE or _DEFAULT_TRANSFORMER_PACKAGE
        _TRANSFORMER_REGISTRY = discover_transformers(package)
    return _TRANSFORMER_REGISTRY


def discover_transformers(package_name: str) -> Dict[str, Type[IDataTransformer]]:
    """
    Discovers and registers all transformer classes within a specified package that implement the
    IDataTransformer interface.

    This function dynamically imports modules from the specified package and introspects them to find all
    classes that are subclasses of IDataTransformer, excluding IDataTransformer itself. It constructs a
    dictionary mapping the class names to the class objects.

    Args:
        package_name (str): The name of the package from which to discover transformer classes.

    Returns:
        Dict[str, Type[IDataTransformer]]: A dictionary with class names as keys and class types as values,
        representing the discovered transformers.

    Raises:
        ImportError: If the package cannot be imported.
        AttributeError: If the package does not have a __path__ attribute, indicating it is not a package.

    Example:
        transformers = discover_transformers('my_transformers_package')
        print(transformers)  # Output might be {'TransformerA': <class 'my_transformers_package.module.TransformerA'>}
    """
    registry: Dict[str, Type[IDataTransformer]] = {}
    package = importlib.import_module(package_name)

    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        module = importlib.import_module(f"{package_name}.{module_name}")
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, IDataTransformer)
                and obj.__module__.startswith(package_name)
                and obj is not IDataTransformer
            ):
                registry[name] = obj

    return registry
