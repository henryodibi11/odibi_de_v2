import pandas as pd
from adlfs import AzureBlobFileSystem
import io
import re
from collections import Counter
from typing import List, Dict, Tuple, Optional
from tqdm.notebook import tqdm


def normalize_excel_column(col_name: str) -> str:
    """Preserve dots in names (e.g. 'Op.System') but strip Excel-style suffixes like '.1', '._2'."""
    col = col_name.strip()
    return re.sub(r"(\.|\._)\d+$", "", col)


def fix_duplicate_excel_headers(raw_cols: List[str]) -> List[str]:
    """Normalize and deduplicate columns: 'Description', 'Description.1' → 'Description', 'Description_2'."""
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
    skip_invalid_files: bool = False,
    file_suffix: str = ".xlsx",
    verbose: bool = True,
    ) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Ingests .xlsx files from ADLS Gen2 using pandas + adlfs.
    - Validates required columns (expected_columns)
    - Tolerates optional columns (optional_columns)
    - Normalizes headers and deduplicates Excel-style suffixes
    - Optionally renames columns
    - Returns merged DataFrame + audit log of any skipped files
    """
    fs = AzureBlobFileSystem(account_name=storage_account, account_key=storage_key)
    full_path = f"{container_name}/{folder_path}".strip("/")

    expected_columns = [col.strip() for col in expected_columns]
    optional_columns = optional_columns or []
    all_required = expected_columns + optional_columns

    audit_log = []
    dfs = []

    try:
        files = [f for f in fs.ls(full_path) if f.endswith(file_suffix)]
    except Exception as e:
        raise FileNotFoundError(f"Could not list files at: {full_path}\n{e}")

    if not files:
        raise FileNotFoundError(f"No Excel files found in: {full_path}")

    print(f"[INFO] Found {len(files)} file(s) in: {full_path}\n")

    for idx, file_path in enumerate(tqdm(files, desc="Processing files", unit="file"), 1):
        try:
            with fs.open(file_path, "rb") as f:
                file_bytes = io.BytesIO(f.read())
                df = pd.read_excel(file_bytes, engine="openpyxl")

            raw_cols = df.columns.tolist()
            actual_cols = fix_duplicate_excel_headers(raw_cols)
            df.columns = actual_cols

            # Schema check: required columns must match exactly
            actual_trimmed = actual_cols[:len(expected_columns)]
            mismatch_found = (
                len(actual_trimmed) != len(expected_columns) or
                any(a != e for a, e in zip(actual_trimmed, expected_columns))
            )

            if mismatch_found:
                print(f"\n[DEBUG] Column mismatch in file: {file_path}")
                for i in range(max(len(actual_trimmed), len(expected_columns))):
                    a = actual_trimmed[i] if i < len(actual_trimmed) else "<MISSING>"
                    e = expected_columns[i] if i < len(expected_columns) else "<MISSING>"
                    status = "✅" if a == e else "❌"
                    print(f" {status} [{i:02}] actual = '{a}' | expected = '{e}'")

                if skip_invalid_files:
                    audit_log.append({
                        "file_path": file_path,
                        "reason": "Column mismatch",
                        "actual_columns": actual_cols
                    })
                    print(f"[SKIPPED] ({idx}/{len(files)}) {file_path} skipped.")
                    continue
                else:
                    raise ValueError(
                        f"[ERROR] Schema mismatch in {file_path} — column names or order do not match expected schema."
                    )

            # Fill missing optional columns
            for col in optional_columns:
                if col not in df.columns:
                    df[col] = None

            # Reorder to expected + optional column order (if all present)
            df = df[[col for col in expected_columns + optional_columns if col in df.columns]]

            # Optional column renaming
            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)

            dfs.append(df)
            if verbose:
                print(f"[SUCCESS] ({idx}/{len(files)}) {file_path} loaded")

        except Exception as e:
            print(f"[FAIL] ({idx}/{len(files)}) Error reading {file_path}: {e}")
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
