
import requests
from odibi_de_v2.logger import log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType

def call_api_core(
    url: str,
    params: dict = None,
    headers: dict = None,
    timeout: int = 30
) -> list[dict]:
    """
    Makes a single REST API request and returns JSON data as a list of dictionaries.

    This function is designed to be a core component for interacting with RESTful APIs,
    particularly useful in data processing environments like Pandas and Spark. It handles
    the HTTP GET request and expects the response in JSON format, typically as a list of records.

    Args:
        url (str): The full URL of the API endpoint to which the request is sent.
        params (dict, optional): A dictionary of query parameters to append to the URL. Default is None.
        headers (dict, optional): A dictionary of HTTP headers to send with the request, such as authorization headers.
            Default is None.
        timeout (int, optional): The timeout for the request in seconds. If the request takes longer than this period,
            a timeout exception is raised. Default is 30 seconds.

    Returns:
        list[dict]: A list of dictionaries, each representing a record in the JSON data returned from the API.

    Raises:
        RuntimeError: If the API request fails for any reason, including network issues, invalid responses, or timeout.

    Example:
        >>> call_api_core("https://api.example.com/data", params={"q": "sales"})
        [{'id': 1, 'product': 'Widget', 'sales': 100}, {'id': 2, 'product': 'Gadget', 'sales': 150}]
    """
    try:
        log_and_optionally_raise(
            module="API",
            component="call_api_core",
            method="GET",
            error_type=ErrorType.NO_ERROR,
            message=f"Calling API: {url}",
            level="INFO"
        )

        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        json_data = response.json()

        # Default assumption: data is inside response.data
        return json_data.get("response", {}).get("data", [])

    except Exception as e:
        raise RuntimeError(f"[call_api_core] API request failed: {e}") from e
