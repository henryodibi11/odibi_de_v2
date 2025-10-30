"""
Logging module for odibi_de_v2.

Provides log sink abstractions for writing transformation execution logs
to various storage backends (Delta Lake, SQL databases, CSV files).
"""

from odibi_de_v2.logging.log_sink import (
    BaseLogSink,
    SparkDeltaLogSink,
    SQLTableLogSink,
    FileLogSink
)

__all__ = [
    'BaseLogSink',
    'SparkDeltaLogSink',
    'SQLTableLogSink',
    'FileLogSink'
]
