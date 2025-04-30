from odibi_de_v2.databricks.utils.dbutils_helpers import get_secret
from odibi_de_v2.databricks.utils.api_auth import load_api_secrets, build_api_auth_headers

def prepare_api_reader_kwargs_from_config(config: dict, dbutils) -> dict:
    """
    Prepares keyword arguments for API ingestion based on a configuration dictionary.

    This function constructs the necessary parameters for API data ingestion by assembling a complete URL,
    resolving secrets for authentication, setting up request headers, and handling optional pagination or
    record path settings. It is specifically tailored for use with Databricks' `dbutils` for secret management.

    Args:
        config (dict): Configuration dictionary containing:
            - `connection_config`: A sub-dictionary with keys like `base_url`, `query_params`, and possibly
            `extra_config` for additional settings.
            - `source_path_or_query`: The specific API endpoint path or query.
        dbutils: The Databricks utility object (`dbutils`) used for accessing secrets stored in a secure way.

    Returns:
        dict: A dictionary with the following keys configured for use in API data ingestion:
            - `url`: The fully constructed URL for the API request.
            - `params`: A dictionary of query parameters.
            - `headers`: A dictionary of headers needed for the request, typically including authentication.
            - `pagination_type`: Optional. The type of pagination used by the API, if applicable.
            - `page_size`: Optional. The number of records per page to request, defaults to 5000 if not specified.
            - `record_path`: Optional. The JSON path within the response data to the records of interest.

    Example:
        >>> config = {
            "connection_config": {
                "base_url": "https://api.example.com",
                "query_params": {"limit": 100},
                "extra_config": {"pagination_type": "offset", "page_size": 100}
            },
            "source_path_or_query": "data/v1/records"
        }
        >>> kwargs = prepare_api_reader_kwargs_from_config(config, dbutils)
        >>> print(kwargs)
        {
            'url': 'https://api.example.com/data/v1/records',
            'params': {'limit': 100},
            'headers': {'Authorization': 'Bearer abc123'},
            'pagination_type': 'offset',
            'page_size': 100,
            'record_path': []
        }
    """

    connection_cfg = config.get("connection_config", {})
    query_params = connection_cfg.get("query_params", {})
    base_url = connection_cfg["base_url"]
    path = config["source_path_or_query"]
    full_url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    # Load and apply secrets
    secrets = load_api_secrets(config)
    headers = build_api_auth_headers(secrets)

    if secrets.get("auth_type") == "key":
        api_key_name = connection_cfg.get("api_key_param_name", "api_key")
        query_params[api_key_name] = secrets["credential"]

    # Add extras like pagination type and record path
    extra = connection_cfg.get("extra_config", {})
    return {
        "url": full_url,
        "params": query_params,
        "headers": headers,
        "pagination_type": extra.get("pagination_type"),
        "page_size": extra.get("page_size", 5000),
        "record_path": extra.get("record_path", [])
    }
