# odibi_de_v2/utils/view_naming.py
import inspect
import re
import threading

_thread_local = threading.local()

def get_caller_prefix() -> str:
    """
    Detect the name of the top-level caller function (e.g. process_nkc_germ_dryer_1)
    and return a safe, lowercase prefix for temporary views.
    """
    stack = inspect.stack()
    # Scan up the call stack to find the first function starting with "process_"
    for frame in stack:
        fn = frame.function
        if fn.lower().startswith("process_"):
            safe = re.sub(r"[^0-9a-zA-Z_]+", "_", fn.lower())
            return safe
    # Fallback to thread name to ensure uniqueness
    return getattr(_thread_local, "fallback_prefix", "default")

def scoped_temp_view_name(base: str) -> str:
    """
    Build a unique temp view name with the caller prefix.
    Example:
        base = "weather_df_tmp" → "process_argo_boilers_weather_df_tmp"
    """
    prefix = get_caller_prefix()
    safe_base = re.sub(r"[^0-9a-zA-Z_]+", "_", base.lower())
    return f"{prefix}_{safe_base}"
