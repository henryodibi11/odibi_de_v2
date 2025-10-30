from .manager import HookManager


PRE_READ = "pre_read"
POST_READ = "post_read"
PRE_TRANSFORM = "pre_transform"
POST_TRANSFORM = "post_transform"
PRE_SAVE = "pre_save"
POST_SAVE = "post_save"
ON_ERROR = "on_error"
PIPELINE_START = "pipeline_start"
PIPELINE_END = "pipeline_end"


__all__ = [
    "HookManager",
    "PRE_READ",
    "POST_READ",
    "PRE_TRANSFORM",
    "POST_TRANSFORM",
    "PRE_SAVE",
    "POST_SAVE",
    "ON_ERROR",
    "PIPELINE_START",
    "PIPELINE_END",
]
