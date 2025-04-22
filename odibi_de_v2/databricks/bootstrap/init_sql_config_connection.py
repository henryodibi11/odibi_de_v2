from odibi_de_v2.connector import SQLDatabaseConnection
from odibi_de_v2.core.enums import Framework
from odibi_de_v2.databricks.utils import get_secret


def init_sql_config_connection(
    secret_scope: str,
    host_key: str,
    database: str,
    user_key: str,
    password_key: str
    ) -> SQLDatabaseConnection:

    """
    Initializes and returns a SQLDatabaseConnection using credentials stored in a secret management service.

    This function simplifies the process of setting up a connection to a SQL database by retrieving connection details
    from a specified secret scope. It is particularly useful in environments where security and credential management
    are paramount.

    Args:
        secret_scope (str): The scope within the secret management service where the credentials are stored.
        host_key (str): The key under which the database host address is stored.
        database (str): The name of the database to connect to.
        user_key (str): The key under which the database username is stored.
        password_key (str): The key under which the database password is stored.

    Returns:
        SQLDatabaseConnection: An object representing the database connection, configured with the retrieved credentials.

    Raises:
        KeyError: If any of the keys do not exist in the specified secret scope.
        ConnectionError: If the connection to the database cannot be established.

    Example:
        >>> connector = init_sql_config_connection(
        ...     secret_scope="my_scope",
        ...     host_key="sql_host",
        ...     database="sql_database",
        ...     user_key="sql_user",
        ...     password_key="sql_password"
        ... )
        >>> print(connector)
        SQLDatabaseConnection(host='example_host', database='sql_database', user='example_user', password='****')
    """
    host = get_secret(secret_scope, host_key)
    user = get_secret(secret_scope, user_key)
    password = get_secret(secret_scope, password_key)

    return SQLDatabaseConnection(
        host=host,
        database=database,
        user=user,
        password=password,
        framework=Framework.SPARK
    )
