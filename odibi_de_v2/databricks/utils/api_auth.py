from base64 import b64encode
from odibi_de_v2.databricks.utils.dbutils_helpers import get_secret

def load_api_secrets(secret_config: dict) -> dict:
    """
    Loads API authentication secrets based on a configuration dictionary using a centralized secret retrieval utility.

    This function supports various authentication schemes including API key, token, and basic authentication.
    It retrieves secrets such as credentials, identifiers, connection strings, and database keys based on the
    provided configuration.

    Args:
        secret_config (dict): A dictionary specifying details for secret retrieval, which includes:
            - secret_scope (str): The scope under which the secrets are stored.
            - credential_key (str, optional): Key for retrieving API credentials.
            - identifier_key (str, optional): Key for retrieving identifier for basic auth.
            - connection_string_key (str, optional): Key for retrieving database connection strings.
            - database_key (str, optional): Key for retrieving database access credentials.
            - token_header_name (str, optional): Header name where the token is expected to be placed.
            - auth_type (str, optional): Type of authentication required ('key', 'token', 'basic'). Defaults to 'key'.
            - description (str, optional): Description or notes regarding the API secrets.

    Returns:
        dict: A dictionary containing all resolved secrets and additional metadata. Keys in the dictionary include
        'credential', 'identifier', 'connection_string', 'database', 'token_header_name', 'auth_type',
        and 'description'.

    Example:
        >>> secret_config = {
            "secret_scope": "prod",
            "credential_key": "api_key",
            "identifier_key": "user_id",
            "auth_type": "key",
            "description": "Production API credentials"
        }
        >>> secrets = load_api_secrets(secret_config)
        >>> print(secrets)
    """
    scope = secret_config.get("secret_scope")
    secrets = {}

    if secret_config.get("credential_key"):
        secrets["credential"] = get_secret(scope, secret_config["credential_key"])

    if secret_config.get("identifier_key"):
        secrets["identifier"] = get_secret(scope, secret_config["identifier_key"])

    if secret_config.get("connection_string_key"):
        secrets["connection_string"] = get_secret(scope, secret_config["connection_string_key"])

    if secret_config.get("database_key"):
        secrets["database"] = get_secret(scope, secret_config["database_key"])

    if secret_config.get("token_header_name"):
        secrets["token_header_name"] = secret_config["token_header_name"]

    # Metadata passthrough
    secrets["auth_type"] = secret_config.get("auth_type", "key")
    secrets["description"] = secret_config.get("description")
    return secrets


def build_api_auth_headers(secrets: dict) -> dict:
    """
    Constructs HTTP headers for API authentication based on the provided secrets.

    This function supports three types of authentication: token-based, basic authentication, and key-based.
    It reads the authentication type from the `secrets` dictionary and constructs the appropriate headers.
    For token-based authentication, it uses a custom or default header name to hold the token. For basic authentication,
    it encodes the username and password into a Base64 string. Key-based authentication does not modify headers as it
    assumes credentials are passed as query parameters.

    Args:
        secrets (dict): A dictionary containing authentication details with keys:
            - 'auth_type' (str): Type of authentication ('token', 'basic', or 'key').
            - 'credential' (str): The token or password, depending on the auth_type.
            - 'identifier' (str, optional): The username for basic authentication.
            - 'token_header_name' (str, optional): Header name for the token if auth_type is 'token'.

    Returns:
        dict: A dictionary containing the necessary HTTP headers for the specified authentication type.

    Raises:
        KeyError: If required keys are missing in the `secrets` dictionary for the specified auth_type.

    Example:
        >>> secrets = {
            "auth_type": "basic",
            "identifier": "user123",
            "credential": "password"
        }
        >>> headers = build_api_auth_headers(secrets)
        >>> print(headers)
        {'Authorization': 'Basic dXNlcjEyMzpwYXNzd29yZA=='}
    """
    headers = {}
    auth_type = secrets.get("auth_type", "key")

    if auth_type == "token":
        header_name = secrets.get("token_header_name", "Authorization")
        headers[header_name] = secrets["credential"]

    elif auth_type == "basic":
        user = secrets.get("identifier")
        pwd = secrets.get("credential")
        token = b64encode(f"{user}:{pwd}".encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {token}"

    # key-based does not add headers (credentials go in query params)
    return headers
