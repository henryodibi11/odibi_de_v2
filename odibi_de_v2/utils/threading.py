import math
import logging
import os
from pyspark.sql import SparkSession

def get_dynamic_thread_count(
    multiplier: float = 2.0,
    min_threads: int = 4,
    max_threads: int = 64,
    workload_type: str = "io"
) -> int:
    """
    Dynamically determine optimal thread count based on Spark cluster cores.

    This function works in both Databricks and local Spark environments.

    Args:
        multiplier (float): Scaling factor applied to total cores.
            Recommended:
                - 1.0–1.5 for CPU-bound workloads
                - 2.0–3.0 for I/O-bound workloads
        min_threads (int): Minimum threads to use.
        max_threads (int): Maximum threads to cap at.
        workload_type (str): "io" (I/O-bound) or "cpu" (CPU-bound).
            Used to auto-adjust the multiplier if not explicitly passed.

    Returns:
        int: Recommended number of threads for ThreadPoolExecutor.

    Example:
        >>> max_workers = get_dynamic_thread_count()
        >>> with ThreadPoolExecutor(max_workers=max_workers) as executor:
        ...     ...
    """

    spark = SparkSession.builder.getOrCreate()

    # --- Detect Databricks or Local ---
    in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    env_label = "Databricks" if in_databricks else "Local"

    # --- Try to read Spark configs ---
    try:
        total_cores = spark.sparkContext.defaultParallelism
    except Exception:
        total_cores = os.cpu_count() or 8
    # --- Auto-select multiplier if not manually set ---
    if workload_type == "cpu" and multiplier == 2.0:
        multiplier = 1.0  # conservative default for CPU-heavy work
    elif workload_type == "io" and multiplier == 2.0:
        multiplier = 2.0  # leave as is, since bronze ingestion is I/O bound

    # --- Compute thread count ---
    dynamic_threads = max(
        min_threads,
        min(max_threads, math.ceil(total_cores * multiplier))
    )

    # --- Logging ---
    logging.info(
        f"[{env_label}] Detected {total_cores} cores → "
        f"using {dynamic_threads} threads (multiplier={multiplier})"
    )

    return dynamic_threads
