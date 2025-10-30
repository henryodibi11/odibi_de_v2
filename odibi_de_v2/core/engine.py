from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


class Engine(Enum):
    """
    Execution engine for data processing pipelines.

    Attributes:
        SPARK (str): Apache Spark distributed processing engine.
        PANDAS (str): Pandas single-node processing engine.

    Example:
        >>> engine = Engine.SPARK
        >>> engine.value
        'spark'

        >>> engine = Engine.PANDAS
        >>> engine.value
        'pandas'
    """
    SPARK = "spark"
    PANDAS = "pandas"


@dataclass
class ExecutionContext:
    """
    Execution context for dual-engine pipelines.

    Encapsulates all state and dependencies needed to run a data pipeline,
    including engine selection, environment configuration, Spark session,
    SQL provider, logger, hooks, and extensibility via extras.

    Attributes:
        engine (Engine): The execution engine (SPARK or PANDAS).
        project (str): Project identifier for multi-tenant environments.
        env (str): Environment identifier (e.g., 'dev', 'prod').
        spark (Optional): SparkSession instance (required for SPARK engine).
        sql_provider (Optional): SQL query provider for dynamic query generation.
        logger (Optional): Logger instance for structured logging.
        hooks (Optional[HookManager]): Hook manager for event-driven workflows.
        extras (Dict[str, Any]): Arbitrary key-value pairs for extensibility.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> ctx = ExecutionContext(
        ...     engine=Engine.SPARK,
        ...     project="energy_efficiency",
        ...     env="dev",
        ...     spark=spark
        ... )
        >>> ctx.engine
        <Engine.SPARK: 'spark'>
        >>> ctx.project
        'energy_efficiency'

        >>> ctx = ExecutionContext(
        ...     engine=Engine.PANDAS,
        ...     project="analytics",
        ...     env="prod",
        ...     extras={"db_path": "/data/warehouse"}
        ... )
        >>> ctx.extras["db_path"]
        '/data/warehouse'

        >>> from odibi_de_v2.hooks import HookManager
        >>> hooks = HookManager()
        >>> ctx = ExecutionContext(
        ...     engine=Engine.SPARK,
        ...     project="pipeline",
        ...     env="dev",
        ...     spark=spark,
        ...     hooks=hooks
        ... )
        >>> ctx.hooks.register("pre_read", lambda p: print("Reading data"))
    """
    engine: Engine
    project: str
    env: str
    spark: Optional[Any] = None
    sql_provider: Optional[Any] = None
    logger: Optional[Any] = None
    hooks: Optional[Any] = None
    extras: Dict[str, Any] = field(default_factory=dict)
