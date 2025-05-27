from typing import List, Dict, Optional, Tuple
import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from odibi_de_v2.core import Framework

def run_excel_ingestion_workflow(
    spark: SparkSession,
    expected_columns: List[str],
    raw_container: str,
    raw_folder_path: str,
    delta_container: str,
    delta_path_prefix: str,
    delta_object_name: str,
    parquet_container: str,
    parquet_path_prefix: str,
    parquet_object_name: str,
    table_name: str,
    database_name: str,
    azure_account_name: str,
    azure_secret_scope: str,
    account_key_key: str,
    file_log_path: Optional[str] = None,
    snapshot_date_filter: Optional[Dict] = None,
    column_mapping: Optional[Dict[str, str]] = None,
    optional_columns: Optional[List[str]] = None,
    date_column: Optional[str] = None,
    register_table: bool = True,
    save_parquet: bool = True,
    skip_invalid_files: bool = False,
    force_reprocess: bool = False,
    verbose: bool = True
) -> Tuple[Optional[SparkDataFrame], List[Dict], pd.DataFrame]:

    from odibi_de_v2.pandas_utils import load_and_normalize_xlsx_folder_pandas
    from odibi_de_v2.storage import SaverProvider
    from odibi_de_v2.core import Framework, DataType
    from odibi_de_v2.connector import AzureBlobConnection
    from odibi_de_v2.databricks import get_secret, DeltaTableManager

    """
    Generic ingestion workflow for Excel files stored in ADLS Gen2.
    Supports snapshot-based filtering, Delta Lake save, optional Parquet export,
    file log caching to avoid redundant reprocessing, and table registration.

    Returns:
        - Spark DataFrame (or None if no new data),
        - audit_log (List[Dict] of success/failure info),
        - file_log_df (DataFrame of file processing history)

    Example:
        df, audit_log, file_log = run_excel_ingestion_workflow(
            spark=spark,
            expected_columns=expected_columns,
            snapshot_date_filter={
                "column": "Sched_start",
                "date_pattern": r"(\\d{8})",
                "date_format": "%m%d%Y",
                "window_days": 6
            },
            column_mapping=column_mapping,
            date_column="Sched_start",
            raw_container="my-container",
            raw_folder_path="raw-data/folder",
            delta_container="my-container",
            delta_path_prefix="delta/path",
            delta_object_name="my_delta_table",
            parquet_container="my-container",
            parquet_path_prefix="parquet/path",
            parquet_object_name="my_file.parquet",
            table_name="my_table",
            database_name="my_db",
            file_log_path="logs/my_log.csv",
            azure_account_name="myaccount",
            azure_secret_scope="MyVault",
            account_key_key="MyKey",
            register_table=True,
            save_parquet=True
        )
    """
    account_key = get_secret(azure_secret_scope, account_key_key)

    df, audit_log, file_log_df = load_and_normalize_xlsx_folder_pandas(
        container_name=raw_container,
        storage_account=azure_account_name,
        storage_key=account_key,
        folder_path=raw_folder_path,
        expected_columns=expected_columns,
        optional_columns=optional_columns,
        column_mapping=column_mapping,
        snapshot_date_filter=snapshot_date_filter,
        skip_invalid_files=skip_invalid_files,
        file_log_path=file_log_path,
        force_reprocess=force_reprocess,
        verbose=verbose
    )

    if df.empty:
        print("[INFO] No new data to process.")
        return None, audit_log, file_log_df

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    df_spark = spark.createDataFrame(df)

    saver = SaverProvider(
        AzureBlobConnection(
            account_name=azure_account_name,
            account_key=account_key,
            framework=Framework.SPARK
        )
    )

    saver.save(
        df=df_spark,
        data_type=DataType.DELTA,
        container=delta_container,
        path_prefix=delta_path_prefix,
        object_name=delta_object_name,
        spark=spark,
        mode="append",
        options={"overwriteSchema": "true"}
    )

    if register_table:
        delta_path = saver.connector.get_file_path(
            container=delta_container,
            path_prefix=delta_path_prefix,
            object_name=delta_object_name
        )
        DeltaTableManager(spark, table_or_path=delta_path, is_path=True).register_table(
            table_name=table_name, database=database_name
        )

    if save_parquet:
        SaverProvider(
            AzureBlobConnection(
                account_name=azure_account_name,
                account_key=account_key,
                framework=Framework.PANDAS
            )
        ).save(
            df=df,
            data_type=DataType.PARQUET,
            container=parquet_container,
            path_prefix=parquet_path_prefix,
            object_name=parquet_object_name
        )

    return df_spark, audit_log, file_log_df
