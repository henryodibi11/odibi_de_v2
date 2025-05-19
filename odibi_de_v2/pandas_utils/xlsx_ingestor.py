import pandas as pd
from adlfs import AzureBlobFileSystem
import io
import re
from collections import Counter
from typing import List, Dict, Tuple
from tqdm.notebook import tqdm


def normalize_excel_column(col_name: str) -> str:
    """
    Normalize by removing Excel-style suffixes like '.1' or '._2' from duplicates,
    but preserve valid periods within column names like 'Op.System Cond.'
    """
    col = col_name.strip()
    return re.sub(r"(\.|\._)\d+$", "", col)


def fix_duplicate_excel_headers(raw_cols: List[str]) -> List[str]:
    """
    Normalize headers and deduplicate them by appending _2, _3, etc.
    """
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
    column_mapping: Dict[str, str] = None,
    skip_invalid_files: bool = False,
    file_suffix: str = ".xlsx",
    verbose: bool = True,
    ) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Reads and merges .xlsx files from ADLS Gen2 using adlfs + pandas.

    Supports:
    - Strict schema validation
    - Column renaming after normalization
    - Optionally skipping invalid files
    - Audit logging of mismatches
    """
    fs = AzureBlobFileSystem(account_name=storage_account, account_key=storage_key)
    full_path = f"{container_name}/{folder_path}".strip("/")

    expected_columns = [col.strip() for col in expected_columns]
    audit_log = []

    try:
        files = [f for f in fs.ls(full_path) if f.endswith(file_suffix)]
    except Exception as e:
        raise FileNotFoundError(f"Could not list files at: {full_path}\n{e}")

    if not files:
        raise FileNotFoundError(f"No Excel files found in: {full_path}")

    print(f"[INFO] Found {len(files)} file(s) in: {full_path}\n")

    dfs = []

    for idx, file_path in enumerate(tqdm(files, desc="Processing files", unit="file"), 1):
        try:
            with fs.open(file_path, "rb") as f:
                file_bytes = io.BytesIO(f.read())
                df = pd.read_excel(file_bytes, engine="openpyxl")

            raw_cols = df.columns.tolist()
            actual_cols = fix_duplicate_excel_headers(raw_cols)
            df.columns = actual_cols

            mismatch_found = (
                len(actual_cols) != len(expected_columns) or
                any(a != e for a, e in zip(actual_cols, expected_columns))
            )

            if mismatch_found:
                print(f"\n[DEBUG] Column mismatch in file: {file_path}")
                for i in range(max(len(actual_cols), len(expected_columns))):
                    a = actual_cols[i] if i < len(actual_cols) else "<MISSING>"
                    e = expected_columns[i] if i < len(expected_columns) else "<MISSING>"
                    status = "✅" if a == e else "❌"
                    print(f" {status} [{i:02}] actual = '{a}' | expected = '{e}'")

                if skip_invalid_files:
                    audit_log.append({
                        "file_path": file_path,
                        "reason": "Column mismatch",
                        "actual_columns": actual_cols
                    })
                    print(f"[SKIPPED] ({idx}/{len(files)}) {file_path} skipped due to mismatch.")
                    continue
                else:
                    raise ValueError(
                        f"[ERROR] Schema mismatch in {file_path} — column names or order do not match expected schema."
                    )

            # Optional column renaming
            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)

            dfs.append(df)
            if verbose:
                print(f"[SUCCESS] ({idx}/{len(files)}) {file_path} loaded")

        except Exception as e:
            error_msg = f"[FAIL] ({idx}/{len(files)}) Error reading {file_path}: {e}"
            print(error_msg)
            if skip_invalid_files:
                audit_log.append({
                    "file_path": file_path,
                    "reason": str(e),
                    "actual_columns": raw_cols
                })
                continue
            else:
                raise

    print(f"\n[INFO] Loaded {len(dfs)} valid file(s). Concatenating now...")
    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return final_df, audit_log
