from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from typing import Optional, Dict, Any
from odibi_de_v2.core import DataReader
from odibi_de_v2.ingestion.pandas.pandas_api_reader import PandasAPIReader


class SparkAPIReader(DataReader):
    """
    Initializes a SparkAPIReader instance to read data from a REST API and convert it to a Spark DataFrame.

    This class serves as a bridge between Pandas and Spark, allowing users to fetch data from an API using the flexible
    PandasAPIReader and then seamlessly convert the data into a Spark DataFrame for further processing in Spark
    environments.

    Args:
        spark (SparkSession): An active Spark session to be used for creating DataFrames.
        url (str): The URL endpoint of the API from which data is to be fetched.
        params (Optional[Dict[str, Any]]): Additional query parameters for the API request. Default is None.
        headers (Optional[Dict[str, str]]): HTTP headers needed for the API request, such as authentication tokens.
            Default is None.
        pagination_type (Optional[str]): The style of pagination used by the API, e.g., 'offset', 'page'.
            Default is None.
        page_size (int): The number of records to request per API call. Defaults to 1000.
        record_path (Optional[list[str]]): A list of keys specifying the path to the data records in the JSON response.
            Default is None.

    Returns:
        SparkDataFrame: A DataFrame containing the data fetched from the API, converted into Spark's DataFrame format.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> reader = SparkAPIReader(
        ...     spark=spark,
        ...     url="https://api.example.com/data",
        ...     params={"start_date": "2024-01-01"},
        ...     headers={"Authorization": "Bearer token"},
        ...     pagination_type="offset",
        ...     page_size=5000,
        ...     record_path=["response", "data"]
        ... )
        >>> spark_df = reader.read_data()
        >>> spark_df.show()
    """

    def __init__(
        self,
        spark: SparkSession,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        pagination_type: Optional[str] = None,
        page_size: int = 1000,
        record_path: Optional[list[str]] = None,
        ):
        """
        Initializes a new instance of the class, setting up a Spark-based data reader with optional pagination and
        HTTP customization.

        Args:
            spark (SparkSession): The Spark session to be used for data processing.
            url (str): The URL from which to fetch the data.
            params (Optional[Dict[str, Any]]): Additional parameters to pass in the HTTP request. Defaults to None.
            headers (Optional[Dict[str, str]]): HTTP headers to use for the request. Defaults to None.
            pagination_type (Optional[str]): The type of pagination to use, if any. Defaults to None.
            page_size (int): The number of records to fetch per page/request. Defaults to 1000.
            record_path (Optional[list[str]]): Path within the JSON response to locate the desired data.
                Defaults to None.

        Raises:
            ValueError: If the provided `pagination_type` is not supported.

        Example:
            >>> spark_session = SparkSession.builder.appName("ExampleApp").getOrCreate()
            >>> reader = MyClass(
            ...    spark=spark_session,
            ...    url="http://api.example.com/data",
            ...    params={"query": "value"},
            ...    headers={"Authorization": "Bearer token"},
            ...    pagination_type="offset",
            ...    page_size=500,
            ...    record_path=["response", "data"]
                )
        """
        self.spark = spark
        self.pandas_reader = PandasAPIReader(
            url=url,
            params=params,
            headers=headers,
            pagination_type=pagination_type,
            page_size=page_size,
            record_path=record_path,
        )

    def read_data(self, **kwargs) -> SparkDataFrame:
        """
        Reads data using a PandasAPIReader and converts it to a Spark DataFrame.

        This method utilizes the configured PandasAPIReader to fetch data based on the provided keyword arguments.
        The fetched data, initially in a pandas DataFrame format, is then converted into a Spark DataFrame using the
        current Spark session available in the instance.

        Args:
            **kwargs: Arbitrary keyword arguments passed directly to the PandasAPIReader's `read_data` method. These
            arguments are typically used to specify data filters, data source identifiers, or other API-specific
            parameters.

        Returns:
            SparkDataFrame: A Spark DataFrame containing the data fetched and converted from the PandasAPIReader.

        Raises:
            HTTPError: If the API call fails.
            ValueError: If the conversion to Spark DataFrame fails due to data compatibility issues.

        Example:
            >>> spark_manager = SparkDataManager(spark, PandasAPIReader())
            >>> df = spark_manager.read_data(start_date='2020-01-01', end_date='2020-01-31')
            >>> df.show()
        """
        pandas_df = self.pandas_reader.read_data(**kwargs)
        return self.spark.createDataFrame(pandas_df)
