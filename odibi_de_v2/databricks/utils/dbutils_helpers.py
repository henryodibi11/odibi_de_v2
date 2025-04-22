
def get_secret(scope: str, key: str) -> str:
    """
    Retrieve a secret value from a specified Databricks secret scope.

    This function abstracts the complexity of accessing secrets stored in
    Databricks, allowing for easy retrieval of sensitive information such as
    API keys or database credentials securely stored in Databricks secret scopes.

    Args:
        scope (str): The name of the Databricks secret scope from which to retrieve the secret.
        key (str): The specific key within the given scope that corresponds to the secret value.

    Returns:
        str: The secret value associated with the provided key within the specified scope.

    Raises:
        Exception: If the secret scope or key does not exist or if there's an issue with
            the Databricks environment or DBUtils.

    Example:
        >>> from databricks.utils.dbutils_helpers import get_secret
        >>> account_name = get_secret("GOATKeyVault", "GoatBlobStorageName")
        >>> account_key = get_secret("GOATKeyVault", "GoatBlobStorageKey")

    Note:
        This function requires that the Databricks environment is properly configured with DBUtils and
            SparkSession available.
    """
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return dbutils.secrets.get(scope, key)
