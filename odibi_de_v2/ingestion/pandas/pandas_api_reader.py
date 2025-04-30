import requests
import pandas as pd
from typing import Optional, Dict, Any
from odibi_de_v2.core import DataReader
from odibi_de_v2.utils import enforce_types, log_call
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType


class PandasAPIReader(DataReader):
    """
        Initializes a reader to fetch data from a REST API and convert it into a Pandas DataFrame.

        This class handles both simple and paginated API requests. It supports extracting data from nested JSON
        structures via a specified record path. The class can handle different pagination styles including 'offset'
        and 'page'.

        Args:
            url (str): The URL of the API endpoint.
            params (dict, optional): Dictionary of query parameters to append to the API request.
            headers (dict, optional): Dictionary of HTTP headers to send with the API request. Useful for setting
                authorization headers or content types.
            pagination_type (str, optional): Type of pagination used by the API. Supported values are 'offset' and
                'page'. If None, pagination is not handled.
            page_size (int, optional): Number of records to request per page in case of paginated APIs.
                Defaults to 1000.
            record_path (list[str], optional): A list of keys defining the path to the data in a nested JSON response.
                Defaults to an empty list, which means data is at the top level.

        Raises:
            ValueError: If the `record_path` does not lead to a list in the JSON structure.

        Example:
            >>> reader = PandasAPIReader(
            ...     url="https://api.example.com/data",
            ...     params={"start_date": "2024-01-01"},
            ...     headers={"Authorization": "Bearer token"},
            ...     pagination_type="offset",
            ...     page_size=5000,
            ...     record_path=["response", "data"]
            ... )
            >>> df = reader.read_data()
            >>> df.head()

        Note:
            This class requires the `pandas` and `requests` libraries to be installed.
        """



    @enforce_types(strict=False)
    @log_call(module="INGESTION", component="PandasAPIReader")
    @log_exceptions(module="INGESTION", component="PandasAPIReader", error_type=ErrorType.Runtime_Error)
    def __init__(
        self,
        url: str,
        params: dict,
        headers: dict = None,
        pagination_type: str = None,
        page_size: int = 1000,
        record_path: list[str] = None,
        ):
        """
            Initializes a new instance of the API client configuration.

            This constructor sets up the necessary parameters for making API requests including URL, parameters,
            and optional settings for handling headers, pagination, and JSON record paths.

            Args:
                url (str): The base URL for the API endpoint.
                params (dict): Dictionary of query parameters to be sent with each request.
                headers (dict, optional): Dictionary of HTTP headers to send with each request. Defaults to an empty
                dictionary if not provided.
                pagination_type (str, optional): Type of pagination to be used. Can be 'page', 'offset', 'cursor', or
                    None if pagination is not needed. Defaults to None.
                page_size (int, optional): The number of records to request per page in case of pagination.
                    Defaults to 1000.
                record_path (list[str], optional): List of keys representing the path to the data in a nested JSON
                    response. Defaults to an empty list if not provided.

            Returns:
                None: This method does not return any value.

            Example:
                >>> api_client = APIClient(
                        url="https://api.example.com/data",
                        params={"key": "value"},
                        headers={"Authorization": "Bearer token"},
                        pagination_type="page",
                        page_size=100,
                        record_path=["records", "data"]
                    )
        """

        self.url = url
        self.params = params or {}
        self.headers = headers or {}
        self.pagination_type = pagination_type
        self.page_size = page_size
        self.record_path = record_path or []

    def read_data(self, **kwargs) -> pd.DataFrame:
        """
        Reads data from an API and returns it as a Pandas DataFrame.

        This method decides whether to fetch data through a single request or multiple paginated requests based on
        the pagination type specified in the instance. It supports additional parameters that can be passed to modify
        the API request.

        Args:
            **kwargs: Arbitrary keyword arguments that are passed directly to the API request function. These arguments
                could include parameters such as filters, fields to retrieve, and other API-specific options.

        Returns:
            pd.DataFrame: A DataFrame containing the data retrieved from the API.

        Raises:
            ValueError: If the API parameters specified in kwargs are invalid.
            ConnectionError: If there is a problem with the network connection.

        Example:
            >>> api_instance = APIReader(pagination_type=True)
            >>> data_frame = api_instance.read_data(filter_by='date:2023-01-01', fields='name,age')
            >>> print(data_frame.head())
        """
        if self.pagination_type:
            return self._read_paginated()
        else:
            return self._read_single_page()

    def _read_single_page(self) -> pd.DataFrame:
        """
        Fetches data from a specified URL and converts it into a pandas DataFrame.

        This private method retrieves JSON data from a web service using a GET request, extracts records using a helper
        method, and then converts these records into a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the records extracted from the JSON data.

        Raises:
            HTTPError: An error from the requests library if the HTTP request returned an unsuccessful status code.

        Example:
            Assuming `_extract_records` is properly defined and `self.url`, `self.params`, and `self.headers` are set:

            >>> df = self._read_single_page()
            >>> print(df.head())
        """
        response = requests.get(self.url, params=self.params, headers=self.headers)
        response.raise_for_status()
        json_data = response.json()
        records = self._extract_records(json_data)
        return pd.DataFrame.from_records(records)

    def _read_paginated(self) -> pd.DataFrame:
        """
        Fetches and aggregates data across multiple pages from a paginated API into a single DataFrame.

        This method internally handles pagination by continuously requesting data from the API until all records have
        been retrieved. It supports both 'offset' and 'page' based pagination strategies. The method constructs a
        DataFrame from the aggregated records.

        Returns:
            pd.DataFrame: A DataFrame containing all records aggregated across the paginated API responses.

        Raises:
            HTTPError: If the request to the API fails.

        Example:
            Assuming an instance `api_client` of a class with this method, and proper setup of `url`,
            `params`, and `headers`:

            >>> df = api_client._read_paginated()
            >>> print(df.head())
        """
        all_records = []
        page_or_offset = 0

        while True:
            if self.pagination_type == "offset":
                self.params["offset"] = page_or_offset
            elif self.pagination_type == "page":
                self.params["page"] = page_or_offset + 1

            response = requests.get(self.url, params=self.params, headers=self.headers)
            response.raise_for_status()
            json_data = response.json()
            records = self._extract_records(json_data)

            if not records:
                break

            all_records.extend(records)
            page_or_offset += self.page_size

        return pd.DataFrame.from_records(all_records)

    def _extract_records(self, json_data: dict) -> list:
        """
        Extracts a list of records from nested JSON data based on a predefined path.

        This method navigates through the nested JSON structure using a sequence of keys defined in `self.record_path`.
        It retrieves a list of records located at the specified path within the JSON data. If the path does not lead to
        a list, the method raises a `ValueError`.

        Args:
            json_data (dict): The JSON data as a dictionary from which records need to be extracted.

        Returns:
            list: A list of records extracted from the JSON data following the `record_path`.

        Raises:
            ValueError: If the `record_path` does not lead to a list in the JSON structure.

        Example:
            Assuming `self.record_path` is ['data', 'items'], and `json_data` is:
            {
                "data": {
                    "items": [
                        {"id": 1, "name": "Item 1"},
                        {"id": 2, "name": "Item 2"}
                    ]
                }
            }
            Calling `_extract_records(json_data)` would return:
            [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}]
        """
        data = json_data
        for key in self.record_path:
            data = data.get(key, {})
        if isinstance(data, list):
            return data
        raise ValueError(f"Invalid record_path: {self.record_path} — data returned was not a list.")
