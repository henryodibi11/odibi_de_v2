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


def infer_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts object columns to appropriate types:
    - Tries numeric conversion
    - Falls back to string
    - Replaces nulls in string columns with empty string

    This avoids Spark conversion issues and guarantees consistent schema behavior.
    """
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                # Try to convert to numeric (only if fully coercible)
                df[col] = pd.to_numeric(df[col], errors="raise")
            except Exception:
                # Fallback to string and fill nulls with empty string
                df[col] = df[col].astype(str).fillna("")
    return df




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

    Automatically infers column types (numeric, datetime, or string) based on value distribution.
    Supports schema validation, optional filtering, renaming, and audit logging.

    Returns:
        Tuple[pd.DataFrame, List[Dict]]: Concatenated result and audit log of skipped files
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
                df = pd.read_excel(file_bytes, engine="openpyxl", dtype=str)

            raw_cols = df.columns.tolist()
            actual_cols = fix_duplicate_excel_headers(raw_cols)
            df.columns = actual_cols

            # Infer types
            df = infer_column_types(df)

            actual_trimmed = actual_cols[:len(expected_columns)]
            mismatch_found = (
                len(actual_trimmed) != len(expected_columns) or
                any(a != e for a, e in zip(actual_trimmed, expected_columns))
            )

            if mismatch_found:
                if skip_invalid_files:
                    return None, {
                        "file_path": file_path,
                        "reason": "Column mismatch",
                        "actual_columns": actual_cols
                    }
                else:
                    raise ValueError(f"Schema mismatch in {file_path}")

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

                if verbose:
                    print(f"[FILTER] {file_name} → {len(df)} rows between {snapshot_date.date()} and {(snapshot_date + timedelta(days=window_days)).date()}")

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
