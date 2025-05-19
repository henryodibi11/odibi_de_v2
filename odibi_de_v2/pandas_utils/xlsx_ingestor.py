import pandas as pd
from adlfs import AzureBlobFileSystem
import io
import re
from collections import Counter
from typing import List, Dict, Tuple, Optional
from tqdm.notebook import tqdm
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


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


def load_and_normalize_xlsx_folder_pandas(
    container_name: str,
    storage_account: str,
    storage_key: str,
    folder_path: str,
    expected_columns: List[str],
    optional_columns: Optional[List[str]] = None,
    column_mapping: Optional[Dict[str, str]] = None,
    snapshot_date_filter: Optional[Dict] = None,
    skip_invalid_files: bool = False,
    file_suffix: str = ".xlsx",
    verbose: bool = True,
    max_workers: int = 8,
    ) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Loads and normalizes multiple .xlsx files from ADLS Gen2 using pandas and adlfs, with parallel execution.

    This function performs schema validation, column deduplication, optional renaming, 
    and optional row filtering based on snapshot dates in the file name. Invalid files 
    can be skipped with detailed audit logging.

    Args:
        container_name (str): Name of the container (e.g., "digital-manufacturing").
        storage_account (str): Name of the ADLS Gen2 storage account.
        storage_key (str): Access key for the storage account.
        folder_path (str): Folder path within the container where .xlsx files are stored.
        expected_columns (List[str]): List of required column names in the expected order.
        optional_columns (List[str], optional): Columns that may be missing from some files.
            These will be added with None if not present. Defaults to None.
        column_mapping (Dict[str, str], optional): Mapping to rename columns. Keys are original names.
        snapshot_date_filter (Dict, optional): Configuration for filtering rows by snapshot date in filename.
            Expected keys:
                - "column": name of column to filter (e.g., "Sched. start")
                - "date_pattern": regex pattern to extract date (default: r"(\\d{8})")
                - "date_format": Python datetime format (default: "%m%d%Y")
                - "window_days": Number of days to keep after snapshot (default: 6)
        skip_invalid_files (bool): If True, skip files with schema mismatch or errors. Defaults to False.
        file_suffix (str): File suffix to filter by, typically ".xlsx". Defaults to ".xlsx".
        verbose (bool): If True, print progress and status messages. Defaults to True.
        max_workers (int): Number of threads to use for parallel file loading. Defaults to 8.

    Returns:
        Tuple[pd.DataFrame, List[Dict]]: A tuple containing:
            - A merged and filtered pandas DataFrame
            - A list of audit log records for skipped or failed files
    """
    fs = AzureBlobFileSystem(account_name=storage_account, account_key=storage_key)
    full_path = f"{container_name}/{folder_path}".strip("/")

    expected_columns = [col.strip() for col in expected_columns]
    optional_columns = optional_columns or []
    all_required = expected_columns + optional_columns

    try:
        files = [f for f in fs.ls(full_path) if f.endswith(file_suffix)]
    except Exception as e:
        raise FileNotFoundError(f"Could not list files at: {full_path}\n{e}")

    if not files:
        raise FileNotFoundError(f"No Excel files found in: {full_path}")

    print(f"[INFO] Found {len(files)} file(s) in: {full_path}\n")

    audit_log = []
    dfs = []

    def process_file(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
        try:
            file_name = file_path.split("/")[-1]
            with fs.open(file_path, "rb") as f:
                file_bytes = io.BytesIO(f.read())
                df = pd.read_excel(file_bytes, engine="openpyxl", parse_dates=False)

            raw_cols = df.columns.tolist()
            actual_cols = fix_duplicate_excel_headers(raw_cols)
            df.columns = actual_cols

            actual_trimmed = actual_cols[:len(expected_columns)]
            mismatch_found = (
                len(actual_trimmed) != len(expected_columns) or
                any(a != e for a, e in zip(actual_trimmed, expected_columns))
            )

            if mismatch_found:
                debug_info = [f"[DEBUG] {file_path}"]
                for i in range(max(len(actual_trimmed), len(expected_columns))):
                    a = actual_trimmed[i] if i < len(actual_trimmed) else "<MISSING>"
                    e = expected_columns[i] if i < len(expected_columns) else "<MISSING>"
                    status = "✅" if a == e else "❌"
                    debug_info.append(f" {status} [{i:02}] actual = '{a}' | expected = '{e}'")
                if skip_invalid_files:
                    return None, {
                        "file_path": file_path,
                        "reason": "Column mismatch",
                        "actual_columns": actual_cols
                    }
                else:
                    raise ValueError("\n".join(debug_info))

            for col in optional_columns:
                if col not in df.columns:
                    df[col] = None

            if snapshot_date_filter:
                date_pattern = snapshot_date_filter.get("date_pattern", r"(\d{8})")
                date_format = snapshot_date_filter.get("date_format", "%m%d%Y")
                date_column = snapshot_date_filter["column"]
                window_days = snapshot_date_filter.get("window_days", 6)

                match = re.search(date_pattern, file_name)
                if not match:
                    raise ValueError(f"Could not extract snapshot date from: {file_name}")

                snapshot_date = datetime.strptime(match.group(1), date_format)
                df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
                df = df[
                    (df[date_column] >= snapshot_date) &
                    (df[date_column] <= snapshot_date + timedelta(days=window_days))
                ]

            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)

            return df, None

        except Exception as e:
            if skip_invalid_files:
                return None, {
                    "file_path": file_path,
                    "reason": str(e),
                    "actual_columns": raw_cols if 'raw_cols' in locals() else []
                }
            else:
                raise

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in files}

        for idx, future in enumerate(as_completed(futures), 1):
            file = futures[future]
            try:
                result_df, result_log = future.result()
                if result_df is not None:
                    dfs.append(result_df)
                    if verbose:
                        print(f"[SUCCESS] ({idx}/{len(files)}) {file} loaded")
                elif result_log:
                    audit_log.append(result_log)
                    print(f"[SKIPPED] ({idx}/{len(files)}) {file} skipped: {result_log['reason']}")
            except Exception as e:
                print(f"[FAIL] ({idx}/{len(files)}) {file} failed with error: {e}")
                if not skip_invalid_files:
                    raise

    print(f"\n[INFO] Loaded {len(dfs)} valid file(s). Concatenating now...")
    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    return final_df, audit_log