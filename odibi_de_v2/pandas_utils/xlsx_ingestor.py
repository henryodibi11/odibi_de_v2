import pandas as pd
from adlfs import AzureBlobFileSystem
import io, re
from typing import List, Dict, Tuple, Optional
from tqdm.notebook import tqdm
from datetime import datetime, timedelta
from collections import Counter
from urllib.parse import quote
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
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_numeric(df[col], errors="raise")
            except Exception:
                df[col] = df[col].astype(str).fillna("")
    return df

def extract_snapshot_date(file_name: str, pattern: str, fmt: str) -> Optional[datetime]:
    match = re.search(pattern, file_name)
    if match:
        return datetime.strptime(match.group(1), fmt)
    return None

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
    file_log_path: Optional[str] = None,
    force_reprocess: bool = False,
    file_suffix: str = ".xlsx",
    verbose: bool = True,
    max_workers: int = 8,
) -> Tuple[pd.DataFrame, List[Dict], pd.DataFrame]:

    fs = AzureBlobFileSystem(account_name=storage_account, account_key=storage_key)
    full_path = f"{container_name}/{folder_path}".strip("/")

    expected_columns = [col.strip() for col in expected_columns]
    optional_columns = optional_columns or []
    all_required = expected_columns + optional_columns

    file_log_lookup = {}
    file_log_df = pd.DataFrame()
    if file_log_path:
        try:
            with fs.open(f"{container_name}/{file_log_path}", "rb") as f:
                file_log_df = pd.read_csv(f)
                file_log_lookup = {
                    str(row["file_name"]).strip(): row["status"]
                    for _, row in file_log_df.iterrows()
                }
            if verbose:
                print(f"[DEBUG] Loaded {len(file_log_lookup)} previously logged files.")
        except FileNotFoundError:
            if verbose:
                print(f"[DEBUG] No existing file log found at: {file_log_path}")
        except Exception as e:
            print(f"[WARN] Could not read file log: {e}")

    try:
        files = [f for f in fs.ls(full_path) if f.endswith(file_suffix)]
    except Exception as e:
        raise FileNotFoundError(f"Could not list files at: {full_path}\n{e}")
    if not files:
        raise FileNotFoundError(f"No Excel files found in: {full_path}")
    print(f"[INFO] Found {len(files)} file(s) in: {full_path}\n")

    audit_log = []
    results = []

    def process_file(file_path: str):
        file_name = file_path.split("/")[-1].strip()

        if file_log_lookup.get(file_name) == "success" and not force_reprocess:
            if verbose:
                print(f"[SKIP] {file_name} — already processed")
            return None, None

        try:
            with fs.open(file_path, "rb") as f:
                df = pd.read_excel(io.BytesIO(f.read()), engine="openpyxl", dtype=str)

            raw_cols = df.columns.tolist()
            actual_cols = fix_duplicate_excel_headers(raw_cols)
            df.columns = actual_cols
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
                        "file_name": file_name,
                        "reason": "Column mismatch",
                        "actual_columns": actual_cols,
                        "status": "skipped"
                    }
                else:
                    raise ValueError(f"Schema mismatch in {file_name}")

            for col in optional_columns:
                if col not in df.columns:
                    df[col] = None

            snapshot_date = None
            if snapshot_date_filter:
                date_column = snapshot_date_filter["column"]
                date_pattern = snapshot_date_filter.get("date_pattern", r"(\d{8})")
                date_format = snapshot_date_filter.get("date_format", "%m%d%Y")
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

            df["source_file_name"] = file_name
            return df, {
                "file_path": file_path,
                "file_name": file_name,
                "status": "success",
                "snapshot_date": snapshot_date.strftime("%Y-%m-%d") if snapshot_date else None,
                "row_count": len(df),
                "error_msg": None
            }

        except Exception as e:
            if skip_invalid_files:
                return None, {
                    "file_path": file_path,
                    "file_name": file_name,
                    "status": "failed",
                    "snapshot_date": None,
                    "row_count": 0,
                    "error_msg": str(e)
                }
            else:
                raise

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in files}
        for idx, future in enumerate(as_completed(futures), 1):
            file = futures[future]
            try:
                df_result, log_entry = future.result()
                if df_result is not None:
                    results.append(df_result)
                    if verbose:
                        print(f"[SUCCESS] ({idx}/{len(files)}) {file} loaded")
                if log_entry:
                    audit_log.append(log_entry)
            except Exception as e:
                print(f"[FAIL] ({idx}/{len(files)}) {file} failed with error: {e}")
                if not skip_invalid_files:
                    raise

    final_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    final_df.drop_duplicates(inplace=True)

    file_log_df = pd.DataFrame(audit_log)
    if file_log_path and not file_log_df.empty:
        try:
            file_log_csv_path = f"{container_name}/{file_log_path}"
            if fs.exists(file_log_csv_path):
                with fs.open(file_log_csv_path, "rb") as f:
                    existing_df = pd.read_csv(f)
                combined_df = pd.concat([existing_df, file_log_df], ignore_index=True)
                combined_df.drop_duplicates(subset=["file_name"], keep="last", inplace=True)
            else:
                combined_df = file_log_df

            with fs.open(file_log_csv_path, "wb") as f:
                combined_df.to_csv(f, index=False)

            if verbose:
                print(f"[INFO] File log updated: {file_log_path} ({len(combined_df)} total tracked)")

        except Exception as e:
            print(f"[WARNING] Failed to update file log: {e}")

    print(f"\n[INFO] Loaded {len(results)} valid file(s).")
    return final_df, audit_log, file_log_df
