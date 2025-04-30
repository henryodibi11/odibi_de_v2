import importlib
import inspect
from functools import wraps



def resolve_storage_function(method_name: str):
    """
    Dynamically resolves and returns a storage function based on its name from a predefined list of modules.

    This function is useful in scenarios where the storage method needs to be selected at runtime, allowing
    for flexible and configurable data handling strategies.

    Args:
        method_name (str): The name of the function to be resolved. This should match exactly with the
        function name in one of the storage modules.

    Returns:
        Callable: A reference to the resolved function, ready to be called with the appropriate arguments.

    Raises:
        ImportError: If the function specified by `method_name` cannot be found in any of the listed modules.

    Example:
        >>> save_function = resolve_storage_function("save_static_data_from_config")
        >>> save_function(data_frame, config_parameters, spark_session, db_connector)
    """
    modules_to_search = [
        "odibi_de_v2.databricks.storage.delta_savers"
    ]

    for module_path in modules_to_search:
        module = importlib.import_module(module_path)
        if hasattr(module, method_name):
            return getattr(module, method_name)

    raise ImportError(f"Method '{method_name}' not found in storage modules: {modules_to_search}")


def wrap_for_foreach_batch_from_registry(
    function_name: str,
    spark,
    connector,
    config
    ):
    from odibi_de_v2.databricks.storage.function_registry import discover_save_functions
    """
    Wraps a registered save function into a handler compatible with Spark's `foreachBatch` method.

    This function retrieves a save function by name from a registry, then creates and returns a handler that
    can be used with the `foreachBatch` method in Spark structured streaming. The handler will inject necessary
    dependencies such as Spark session, connector, and configuration into the save function during stream processing.

    Args:
        function_name (str): The name of the function registered in the FUNCTION_REGISTRY.
        spark (SparkSession): The Spark session instance to be passed to the save function.
        connector (BaseConnection): The connector instance to be passed to the save function.
        config (dict): A dictionary containing configuration settings to be passed to the save function.

    Returns:
        Callable[[DataFrame, int], None]: A function that takes a DataFrame and a batch ID, suitable for use as
            a `foreachBatch` handler.

    Raises:
        ValueError: If the specified `function_name` is not found in the FUNCTION_REGISTRY.

    Example:
        spark = SparkSession.builder.appName("ExampleApp").getMaster("local").getOrCreate()
        connector = SomeConnector()
        config = {"path": "s3://bucket/data"}
        handler = wrap_for_foreach_batch_from_registry("save_to_s3", spark, connector, config)
        streaming_df.writeStream.foreachBatch(handler).start()
    """
    FUNCTION_REGISTRY = discover_save_functions()
    fn = FUNCTION_REGISTRY.get(function_name)
    if not fn:
        raise ValueError(f"Function '{function_name}' not found in FUNCTION_REGISTRY.")

    def handler(micro_batch_df, batch_id):
        print(f"[foreachBatch] Invoking registered function '{function_name}' for batch {batch_id}")
        return fn(
            df=micro_batch_df,
            spark=spark,
            connector=connector,
            config=config,
        )

    return handler



REQUIRED_PARAMS = {"df", "config", "spark", "connector"}

def validate_save_function_signature(required_params: set = REQUIRED_PARAMS):
    """
    Validates that a decorated function includes all required parameters.

    This function is a decorator factory, which creates a decorator that checks if the decorated function includes a
    specific set of parameters. If any required parameter is missing, it raises a `ValueError`.

    Args:
        required_params (set, optional): A set of strings representing the names of parameters that the decorated
            function must accept. Defaults to `REQUIRED_PARAMS`.

    Returns:
        Callable: A decorator that can be used to decorate a function, ensuring it has the required parameters.

    Raises:
        ValueError: If the decorated function does not include all parameters specified in `required_params`.

    Example:
        @validate_save_function_signature({'user_id', 'data'})
        def process_data(user_id, data, timestamp=None):
            pass

        In the example above, the `process_data` function is checked to ensure it includes 'user_id' and 'data'
        parameters. If either is missing, a `ValueError` will be raised.
    """
    def decorator(func):
        sig = inspect.signature(func)
        func_params = set(sig.parameters.keys())

        if not required_params.issubset(func_params):
            missing = required_params - func_params
            raise ValueError(
                f"Function '{func.__name__}' is missing required parameters: {missing}"
            )

        return func

    return decorator
