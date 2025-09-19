"""
safe_globals.py

Defines a restricted but useful global environment for safely evaluating
string-based lambda functions and other dynamic expressions in odibi_de.

This sandbox removes all builtins and selectively whitelists common modules,
functions, and helpers relevant to data transformations.
"""

import math
import statistics
import datetime
import operator
import re

SAFE_GLOBALS = {
    "__builtins__": {},   # block all dangerous builtins completely

    # 🔹 Math & numeric
    "math": math,
    "statistics": statistics,
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sum": sum,

    # 🔹 Date & time
    "datetime": datetime,

    # 🔹 Regex / text processing
    "re": re,

    # 🔹 Operator helpers (row and nested dict access)
    "operator": operator,
    "itemgetter": operator.itemgetter,
    "attrgetter": operator.attrgetter,
    "methodcaller": operator.methodcaller,

    # 🔹 Built-in types & casting
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,

    # 🔹 Sequence / collection utilities
    "len": len,
    "sorted": sorted,
    "reversed": reversed,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "any": any,
    "all": all,
}
