from typing import List, Dict, Optional, Tuple
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from collections import Counter
import re, warnings


# === Helpers ===
def normalize_excel_column(col_name: str) -> str:
    col = col_name.strip()
    return re.sub(r"(\.|\._)\d+$", "", col)


def fix_duplicate_excel_headers(raw_cols: List[str]) -> List[str]:
    cleaned = [normalize_excel_column(c) for c in raw_cols]
    counts = Counter()
    result = []
    for c in cleaned:
        counts[c] += 1
        result.append(c if counts[c] == 1 else f"{c}_{counts[c]}")
    return result


def filter_files_by_date(
    files: List[str],
    date_pattern: str,
    days_back: int,
) -> List[Tuple[str, datetime]]:
    """Filter files using a date regex and cutoff."""

    # lazy import here
    from odibi_de_v2.core import ErrorType
    from odibi_de_v2.logger import log_and_optionally_raise

    cutoff_date = datetime.today() - timedelta(days=days_back)
    selected = []

    for f in files:
        m = re.search(date_pattern, f)
        if not m:
            continue
        file_dt = None
        for fmt in ("%m%d%Y", "%Y%m%d"):
            try:
                file_dt = datetime.strptime(m.group(1), fmt)
                break
            except ValueError:
                continue
        if file_dt and file_dt >= cutoff_date:
            selected.append((f, file_dt))

    log_and_optionally_raise(
        module="UTILS",
        component="ParquetIngestion",
        method="filter_files_by_date",
        error_type=ErrorType.NO_ERROR,
        message=f"{len(selected)} files selected after applying cutoff ({cutoff_date.date()}).",
        level="INFO",
    )
    return sorted(selected, key=lambda x: x[1])


def load_and_prepare_parquet(
    fpath: str,
    azure_connector_pandas,
    header_row_index: int,
    file_dt: datetime = None
) -> Optional[pd.DataFrame]:
    """Load a parquet file, promote header row, fix headers, attach metadata."""

    # lazy import here
    from odibi_de_v2.core import ErrorType
    from odibi_de_v2.logger import log_and_optionally_raise

    try:
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="load_and_prepare_parquet",
            error_type=ErrorType.NO_ERROR,
            message=f"Attempting to read PARQUET file from: {fpath}",
            level="INFO",
        )

        pdf = pd.read_parquet(fpath, storage_options=azure_connector_pandas.get_storage_options())

        # Promote header row
        header_row = pdf.iloc[header_row_index].tolist()
        pdf = pdf.drop(index=list(range(header_row_index + 1))).reset_index(drop=True)

        # Fix duplicate/illegal headers
        pdf.columns = fix_duplicate_excel_headers(header_row)

        # Add metadata
        pdf["filename"] = fpath
        if file_dt:
            pdf["file_snapshot_date"] = file_dt

        return pdf
    except Exception as e:
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="load_and_prepare_parquet",
            error_type=ErrorType.RUNTIME_ERROR,
            message=f"Failed to load {fpath}: {e}",
            level="ERROR",
        )
        return None


def run_parquet_ingestion_workflow(
    spark: SparkSession,
    azure_connector,
    azure_connector_pandas,
    expected_columns: List[str],
    raw_container: str,
    raw_folder_path: str,
    delta_container: str,
    delta_path_prefix: str,
    delta_object_name: str,
    table_name: str,
    database_name: str,
    column_mapping: Optional[Dict[str, str]] = None,
    snapshot_date_filter: Optional[Dict] = None,
    header_row_index: int = 0,
    register_table: bool = True,
) -> Optional[SparkDataFrame]:

    # lazy imports here
    from odibi_de_v2.core import Framework, DataType, ErrorType
    from odibi_de_v2.storage import SaverProvider
    from odibi_de_v2.databricks import DeltaTableManager
    from odibi_de_v2.utils import ADLSFolderUtils
    from odibi_de_v2.logger import log_and_optionally_raise

    warnings.filterwarnings("ignore", category=FutureWarning)

    # === STEP 1. List parquet files ===
    folder_utils = ADLSFolderUtils(azure_connector, framework=Framework.SPARK, spark=spark)
    all_files = folder_utils.list_files(raw_container, raw_folder_path, ".parquet")

    if not all_files:
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="run_parquet_ingestion_workflow",
            error_type=ErrorType.NO_ERROR,
            message="No parquet files found.",
            level="WARN",
        )
        return None

    # === STEP 2. Filter by cutoff and Load parquet files ===
    pdfs = []
    if snapshot_date_filter:
        days_back = snapshot_date_filter.get("days_back", 15) if snapshot_date_filter else 15
        date_pattern = snapshot_date_filter.get("date_pattern", r"(\d{8})") if snapshot_date_filter else r"(\d{8})"
        selected_files = filter_files_by_date(all_files, date_pattern, days_back)

        for fpath, file_dt in selected_files:
            pdf = load_and_prepare_parquet(fpath, azure_connector_pandas, header_row_index, file_dt)
            if pdf is not None:
                pdfs.append(pdf)
    else:
        selected_files = all_files
        for fpath in selected_files:
            pdf = load_and_prepare_parquet(fpath, azure_connector_pandas, header_row_index)
            if pdf is not None:
            pdfs.append(pdf)
    if not selected_files:
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="run_parquet_ingestion_workflow",
            error_type=ErrorType.NO_ERROR,
            message="No parquet files meet cutoff criteria.",
            level="WARN",
        )
        return None

    
    if not pdfs:
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="run_parquet_ingestion_workflow",
            error_type=ErrorType.NO_ERROR,
            message="No valid parquet data loaded.",
            level="WARN",
        )
        return None

    combined_pdf = pd.concat(pdfs, ignore_index=True)
    log_and_optionally_raise(
        module="UTILS",
        component="ParquetIngestion",
        method="run_parquet_ingestion_workflow",
        error_type=ErrorType.NO_ERROR,
        message=f"Combined DataFrame → {len(combined_pdf)} rows, {len(combined_pdf.columns)} cols",
        level="INFO",
    )

    # === STEP 3. Convert to Koalas ===
    psdf = ps.from_pandas(combined_pdf)

    if column_mapping:
        psdf = psdf.rename(columns=column_mapping)
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="run_parquet_ingestion_workflow",
            error_type=ErrorType.NO_ERROR,
            message=f"Applied column mapping → {len(psdf.columns)} cols",
            level="INFO",
        )

    # Ensure expected columns
    expected_columns_mapped = [column_mapping.get(col, col) for col in expected_columns] if column_mapping else expected_columns
    for col in expected_columns_mapped:
        if col not in psdf.columns:
            psdf[col] = ""
            log_and_optionally_raise(
                module="UTILS",
                component="ParquetIngestion",
                method="run_parquet_ingestion_workflow",
                error_type=ErrorType.NO_ERROR,
                message=f"Added missing col: {col}",
                level="WARN",
            )

    psdf = psdf[expected_columns_mapped + ["filename", "file_snapshot_date"]]

    # === STEP 4. Convert to Spark ===
    df_spark = psdf.to_spark()

    for c, t in df_spark.dtypes:
        if t == "void":
            df_spark = df_spark.withColumn(c, F.lit(None).cast("string"))
    # === STEP 6. Save to Delta ===
    saver = SaverProvider(azure_connector)
    saver.save(
        df=df_spark,
        data_type=DataType.DELTA,
        container=delta_container,
        path_prefix=delta_path_prefix,
        object_name=delta_object_name,
        spark=spark,
        mode="overwrite",
        options={"overwriteSchema": "true", "mergeSchema": "true"},
    )

    if register_table:
        delta_path = saver.connector.get_file_path(
            container=delta_container,
            path_prefix=delta_path_prefix,
            object_name=delta_object_name,
        )
        DeltaTableManager(spark, table_or_path=delta_path, is_path=True).register_table(
            table_name=table_name, database=database_name
        )
        log_and_optionally_raise(
            module="UTILS",
            component="ParquetIngestion",
            method="run_parquet_ingestion_workflow",
            error_type=ErrorType.NO_ERROR,
            message=f"Registered {database_name}.{table_name}",
            level="INFO",
        )

    log_and_optionally_raise(
        module="UTILS",
        component="ParquetIngestion",
        method="run_parquet_ingestion_workflow",
        error_type=ErrorType.NO_ERROR,
        message="Ingestion completed successfully.",
        level="INFO",
    )

    return df_spark
