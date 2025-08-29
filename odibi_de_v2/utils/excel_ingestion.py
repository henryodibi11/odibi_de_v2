from typing import List, Dict, Optional, Tuple
import pyspark.pandas as ps
import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from datetime import datetime, timedelta
import re, warnings, fsspec
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter


# === Helpers for fixing duplicate Excel columns ===
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


def run_excel_ingestion_workflow(
    spark: SparkSession,
    azure_connector,
    azure_connector_pandas,
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
    column_mapping: Optional[Dict[str, str]] = None,
    snapshot_date_filter: Optional[Dict] = None,
    max_threads: int = 4,
    register_table: bool = True,
    save_parquet: bool = True,
    verbose: bool = True,
) -> Tuple[Optional[SparkDataFrame], pd.DataFrame]:

    from odibi_de_v2.core import Framework, DataType
    from odibi_de_v2.connector import AzureBlobConnection
    from odibi_de_v2.storage import SaverProvider
    from odibi_de_v2.databricks import DeltaTableManager
    """
    Multithreaded Excel ingestion workflow for ADLS Gen2 using Pandas + fsspec.
    - Filters snapshots by filename date and/or column date
    - Loads files in parallel with ThreadPoolExecutor
    - Fixes duplicate Excel headers
    - Applies column_mapping to ensure Delta-safe schema
    - Saves results as Delta + Parquet
    """

    warnings.filterwarnings("ignore", category=FutureWarning)

    # === STEP 1. Apply Spark storage config ===
    for key, value in azure_connector.get_storage_options().items():
        spark.conf.set(key, value)
    account_name = azure_connector.account_name

    # === STEP 2. Filtering config ===
    days_back = snapshot_date_filter.get("days_back", 30) if snapshot_date_filter else 30
    date_pattern = snapshot_date_filter.get("date_pattern", r"(\d{8})") if snapshot_date_filter else r"(\d{8})"
    cutoff_date = datetime.today() - timedelta(days=days_back)

    if verbose:
        print(f"[INFO] Starting ingestion with days_back={days_back}, cutoff={cutoff_date.date()}")

    # === STEP 3. List Excel files ===
    base_path = f"abfss://{raw_container}@{account_name}.dfs.core.windows.net/{raw_folder_path}/"
    all_files = dbutils.fs.ls(base_path)

    selected_files = []
    for f in all_files:
        if not f.name.endswith(".xlsx"):
            continue
        m = re.search(date_pattern, f.name)
        if not m:
            continue
        try:
            file_dt = datetime.strptime(m.group(1), "%m%d%Y")
        except ValueError:
            try:
                file_dt = datetime.strptime(m.group(1), "%Y%m%d")
            except ValueError:
                continue
        if file_dt >= cutoff_date:
            selected_files.append((f.path, file_dt))

    selected_files = sorted(selected_files, key=lambda x: x[1])

    if verbose:
        print(f"[INFO] {len(selected_files)} Excel files selected for processing:")
        for fp, dt in selected_files:
            print(f"   - {fp} (date={dt.date()})")

    if not selected_files:
        return None, pd.DataFrame()

    # === STEP 4. Multithreaded Excel loading with fsspec ===
    storage_opts = azure_connector_pandas.get_storage_options()

    def load_excel(file_path, file_dt):
        try:
            if verbose:
                print(f"[THREAD] Loading {file_path}")
            with fsspec.open(file_path, "rb", **storage_opts) as f:
                pdf = pd.read_excel(f)

            # ✅ Fix duplicate headers immediately
            raw_cols = pdf.columns.tolist()
            pdf.columns = fix_duplicate_excel_headers(raw_cols)

            pdf["filename"] = file_path
            pdf["file_snapshot_date"] = file_dt
            if verbose:
                print(f"[THREAD] Completed {file_path} ({len(pdf)} rows)")
            return pdf
        except Exception as e:
            print(f"[WARN] Failed to load {file_path}: {e}")
            return None

    pdfs = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(load_excel, fp, dt) for fp, dt in selected_files]
        for future in as_completed(futures):
            pdf = future.result()
            if pdf is not None:
                pdfs.append(pdf)

    if not pdfs:
        print("[INFO] No valid Excel data loaded.")
        return None, pd.DataFrame()

    combined_pdf = pd.concat(pdfs, ignore_index=True)

    # === STEP 5. Normalize schema (Koalas) ===
    psdf = ps.from_pandas(combined_pdf)

    if column_mapping:
        psdf = psdf.rename(columns=column_mapping)

    expected_columns_mapped = [column_mapping.get(col, col) for col in expected_columns]
    for col in expected_columns_mapped:
        if col not in psdf.columns:
            psdf[col] = None

    psdf = psdf[expected_columns_mapped + ["filename", "file_snapshot_date"]]

    # === STEP 6. Row-level filtering (optional) ===
    if snapshot_date_filter and "column" in snapshot_date_filter:
        col_name = column_mapping.get(snapshot_date_filter["column"], snapshot_date_filter["column"]) \
                    if column_mapping else snapshot_date_filter["column"]

        if col_name in psdf.columns:
            fmt = snapshot_date_filter.get("date_format", "%m%d%Y")
            psdf[col_name] = ps.to_datetime(psdf[col_name], format=fmt, errors="coerce")
            if "window_days" in snapshot_date_filter:
                cutoff = pd.Timestamp.today() - pd.Timedelta(days=snapshot_date_filter["window_days"])
                before = len(psdf)
                psdf = psdf[psdf[col_name] >= cutoff]
                if verbose:
                    print(f"[INFO] Row-level filter: reduced {before} → {len(psdf)} rows")

    # === STEP 7. Convert to Spark DF & enforce mapping ===
    df_spark = psdf.to_spark()
    if column_mapping:
        df_spark = df_spark.select(
            *[df_spark[col].alias(column_mapping.get(col, col)) for col in df_spark.columns]
        )

    # === STEP 8. Save to Delta (overwrite) ===
    saver = SaverProvider(
        AzureBlobConnection(
            account_name=account_name,
            account_key=azure_connector.account_key,
            framework=Framework.SPARK
        )
    )

    if verbose:
        print(f"[INFO] Saving to Delta: {delta_container}/{delta_path_prefix}/{delta_object_name}")

    saver.save(
        df=df_spark,
        data_type=DataType.DELTA,
        container=delta_container,
        path_prefix=delta_path_prefix,
        object_name=delta_object_name,
        spark=spark,
        mode="overwrite",
        options={"overwriteSchema": "true", "mergeSchema": "true"}
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
        if verbose:
            print(f"[INFO] Registered table {database_name}.{table_name}")

    df_full_spark = spark.sql(f"SELECT * FROM {database_name}.{table_name}")
    df_full = df_full_spark.toPandas()

    if save_parquet:
        if verbose:
            print(f"[INFO] Saving Parquet copy: {parquet_container}/{parquet_path_prefix}/{parquet_object_name}")
        SaverProvider(
            AzureBlobConnection(
                account_name=account_name,
                account_key=azure_connector.account_key,
                framework=Framework.PANDAS
            )
        ).save(
            df=df_full,
            data_type=DataType.PARQUET,
            container=parquet_container,
            path_prefix=parquet_path_prefix,
            object_name=parquet_object_name
        )

    if verbose:
        print("[INFO] Ingestion completed successfully.")

    return df_full_spark, df_full
