from typing import Callable, Dict, List, Optional, Any
from collections import defaultdict


class HookManager:
    """
    Event-driven hook manager for lifecycle callbacks.

    Enables extensible, decoupled pipelines by allowing registration of
    callbacks at key lifecycle events. Supports filtering by project,
    layer, and engine to enable targeted hooks.

    Standard Events:
        - pre_read: Before data ingestion starts
        - post_read: After data is successfully read
        - pre_transform: Before transformation logic executes
        - post_transform: After transformation completes
        - pre_save: Before data is written to storage
        - post_save: After data is successfully saved
        - on_error: When an exception occurs during pipeline execution
        - pipeline_start: At the beginning of pipeline orchestration
        - pipeline_end: At the end of pipeline orchestration

    Methods:
        register: Register a callback for a specific event with optional filters.
        emit: Trigger all callbacks for an event with a payload.
        list_hooks: List all registered hooks for debugging.

    Example:
        >>> hooks = HookManager()
        >>> def log_read(payload):
        ...     print(f"Reading {payload['table']}")
        >>> hooks.register("pre_read", log_read)
        >>> hooks.emit("pre_read", {"table": "bronze.raw_data"})
        Reading bronze.raw_data

        >>> def validate_schema(payload):
        ...     df = payload["df"]
        ...     required_cols = ["id", "timestamp"]
        ...     missing = [c for c in required_cols if c not in df.columns]
        ...     if missing:
        ...         raise ValueError(f"Missing columns: {missing}")
        >>> hooks.register("post_read", validate_schema)
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([(1, "2023-01-01")], ["id", "timestamp"])
        >>> hooks.emit("post_read", {"df": df})

        >>> def bronze_only_callback(payload):
        ...     print("Bronze layer processing")
        >>> hooks.register(
        ...     "pre_transform",
        ...     bronze_only_callback,
        ...     filters={"layer": "bronze"}
        ... )
        >>> hooks.emit("pre_transform", {"layer": "bronze"})
        Bronze layer processing
        >>> hooks.emit("pre_transform", {"layer": "silver"})

        >>> def spark_optimizations(payload):
        ...     print("Applying Spark broadcast joins")
        >>> hooks.register(
        ...     "pre_transform",
        ...     spark_optimizations,
        ...     filters={"engine": "spark"}
        ... )
        >>> hooks.emit("pre_transform", {"engine": "spark"})
        Applying Spark broadcast joins

        >>> hooks = HookManager()
        >>> hooks.register("pre_read", lambda p: print("Hook 1"))
        >>> hooks.register("pre_read", lambda p: print("Hook 2"))
        >>> hooks.register("post_save", lambda p: print("Save done"))
        >>> registered = hooks.list_hooks()
        >>> len(registered["pre_read"])
        2
        >>> len(registered["post_save"])
        1
    """

    def __init__(self):
        """Initialize the HookManager with empty hook registry."""
        self._hooks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def register(
        self,
        event: str,
        callback: Callable[[Dict[str, Any]], None],
        filters: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Register a callback for a specific event.

        Args:
            event (str): Event name (e.g., 'pre_read', 'post_transform').
            callback (Callable): Function to execute when event is emitted.
                Should accept a single dict payload argument.
            filters (Optional[Dict[str, str]]): Filter conditions (project,
                layer, engine). Hook only fires if payload matches all filters.

        Returns:
            None

        Example:
            >>> hooks = HookManager()
            >>> def my_callback(payload):
            ...     print(f"Event triggered with {payload}")
            >>> hooks.register("pre_read", my_callback)
            >>> hooks.register(
            ...     "pre_transform",
            ...     my_callback,
            ...     filters={"layer": "bronze", "project": "energy"}
            ... )
        """
        self._hooks[event].append({
            "callback": callback,
            "filters": filters or {}
        })

    def emit(self, event: str, payload: Dict[str, Any]) -> None:
        """
        Trigger all registered callbacks for an event.

        Executes callbacks in registration order. Only callbacks whose filters
        match the payload are executed.

        Args:
            event (str): Event name to trigger.
            payload (Dict[str, Any]): Data to pass to callbacks. Used for
                both filtering and callback arguments.

        Returns:
            None

        Example:
            >>> hooks = HookManager()
            >>> def log_event(p):
            ...     print(f"Received: {p.get('message')}")
            >>> hooks.register("test_event", log_event)
            >>> hooks.emit("test_event", {"message": "Hello"})
            Received: Hello

            >>> def filtered_callback(p):
            ...     print("Bronze only")
            >>> hooks.register(
            ...     "layer_event",
            ...     filtered_callback,
            ...     filters={"layer": "bronze"}
            ... )
            >>> hooks.emit("layer_event", {"layer": "bronze"})
            Bronze only
            >>> hooks.emit("layer_event", {"layer": "silver"})
        """
        if event not in self._hooks:
            return

        for hook in self._hooks[event]:
            filters = hook["filters"]
            if self._matches_filters(payload, filters):
                hook["callback"](payload)

    def list_hooks(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        List all registered hooks for debugging.

        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping event names
                to lists of hook metadata (callback and filters).

        Example:
            >>> hooks = HookManager()
            >>> hooks.register("pre_read", lambda p: None)
            >>> hooks.register("post_save", lambda p: None, filters={"layer": "gold"})
            >>> registered = hooks.list_hooks()
            >>> "pre_read" in registered
            True
            >>> "post_save" in registered
            True
            >>> registered["post_save"][0]["filters"]
            {'layer': 'gold'}
        """
        return dict(self._hooks)

    def _matches_filters(
        self,
        payload: Dict[str, Any],
        filters: Dict[str, str]
    ) -> bool:
        """
        Check if payload matches all filter conditions.

        Args:
            payload (Dict[str, Any]): Event payload data.
            filters (Dict[str, str]): Filter key-value pairs.

        Returns:
            bool: True if all filters match payload values, False otherwise.
        """
        for key, value in filters.items():
            if payload.get(key) != value:
                return False
        return True
