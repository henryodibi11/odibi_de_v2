from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import time
import json
import inspect
from typing import Optional, Any
from pyspark.sql.utils import AnalysisException
from odibi_de_v2.logger import get_logger, log_and_optionally_raise
from odibi_de_v2.core import DataType, ErrorType, Engine, ExecutionContext
from odibi_de_v2.utils import get_dynamic_thread_count
from odibi_de_v2.odibi_functions import REGISTRY
from odibi_de_v2.hooks import HookManager
from odibi_de_v2.logging import BaseLogSink, SparkDeltaLogSink
import os


class TransformationRunnerFromConfig:
    """
    Global orchestrator that executes transformation functions
    defined in TransformationRegistry and logs all runs
    to config_driven.TransformationRunLog (Delta).
    
    Updated for v2.0 to support the new TransformationRegistry table
    with JSON fields for inputs, constants, and outputs.
    
    Supports dual-engine execution (Spark/Pandas) with hooks and pluggable log sinks.
    """
    def __init__(
        self,
        sql_provider,
        project: str,
        env: str = "qat",
        log_level: str = "ERROR",
        max_workers: int = min(32, (os.cpu_count() or 1) + 4),
        layer: str = 'Silver',
        engine: str = "spark",
        hooks: Optional[HookManager] = None,
        log_sink: Optional[BaseLogSink] = None,
        **kwargs
        ):
        self.sql_provider = sql_provider
        self.project = project
        self.env = env
        self.log_level = log_level
        self.max_workers = max_workers
        self.layer = layer
        self.engine = Engine.SPARK if engine.lower() == "spark" else Engine.PANDAS
        self.hooks = hooks or HookManager()
        self.kwargs = kwargs
        
        # Initialize Spark session (optional if engine is pandas)
        if self.engine == Engine.SPARK:
            self.spark = SparkSession.getActiveSession()
            if not self.spark:
                raise RuntimeError("SparkSession is required for engine='spark' but no active session found")
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS config_driven")
        else:
            self.spark = SparkSession.getActiveSession() if SparkSession.getActiveSession() else None
        
        # Initialize log sink
        if log_sink:
            self.log_sink = log_sink
        elif self.spark:
            self.log_sink = SparkDeltaLogSink(self.spark, "config_driven.TransformationRunLog")
        else:
            self.log_sink = None
        
        # Initialize logger
        self.logger = get_logger()
        self.logger.set_log_level(log_level)


    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------
    def _fetch_configs(self):
        """
        Fetch transformation configs from TransformationRegistry.
        Handles both new TransformationRegistry and legacy TransformationConfig tables.
        """
        # Try new TransformationRegistry first
        try:
            query = f"""
            SELECT
                transformation_id as id,
                project,
                environment as env,
                layer,
                entity_1 as plant,
                entity_2 as asset,
                module,
                [function],
                inputs,
                constants,
                outputs,
                enabled
            FROM TransformationRegistry
            WHERE LOWER(project) = LOWER('{self.project}') 
            AND enabled = 1 
            AND environment = '{self.env}'
            AND layer = '{self.layer}'
            """
            
            self.logger.log("info", f"Fetching configurations from TransformationRegistry for {self.project} ({self.env})")
            
            source_target = self.sql_provider.read(
                data_type=DataType.SQL,
                container="",
                path_prefix="",
                object_name=query,
                spark=self.spark)
            
            configs = []
            for row in source_target.collect():
                config = row.asDict()
                
                # Parse JSON fields (inputs, constants, outputs)
                try:
                    config['inputs'] = json.loads(config['inputs']) if config.get('inputs') else []
                except (json.JSONDecodeError, TypeError):
                    config['inputs'] = []
                
                try:
                    config['constants'] = json.loads(config['constants']) if config.get('constants') else {}
                except (json.JSONDecodeError, TypeError):
                    config['constants'] = {}
                
                try:
                    config['outputs'] = json.loads(config['outputs']) if config.get('outputs') else []
                except (json.JSONDecodeError, TypeError):
                    config['outputs'] = []
                
                configs.append(config)
            
            return configs
            
        except Exception as e:
            # Fall back to legacy TransformationConfig table
            self.logger.log("warning", f"TransformationRegistry not found, using legacy TransformationConfig table")
            
            query = f"""
            SELECT
                *
            FROM TransformationConfig
            WHERE project = '{self.project}' AND enabled = 1 AND env = '{self.env}'
                AND layer = '{self.layer}'
            """
            source_target = self.sql_provider.read(
                data_type=DataType.SQL,
                container="",
                path_prefix="",
                object_name=query,
                spark=self.spark)
            
            configs = []
            for row in source_target.collect():
                config = row.asDict()
                # Convert legacy format to new format
                config['inputs'] = [config.get('input_table')] if config.get('input_table') else []
                config['constants'] = {}
                config['outputs'] = [{'table': config.get('target_table'), 'mode': 'overwrite'}] if config.get('target_table') else []
                configs.append(config)
            
            return configs

    def _log(self, data: list):
        if not self.log_sink:
            return
        
        records = [row.asDict() for row in data]
        self.log_sink.write(records)

    def _log_start(self, cfg):
        data = [Row(
            transformation_id=str(cfg["id"]),
            project=cfg["project"],
            plant=cfg.get("plant"),
            asset=cfg.get("asset"),
            start_time=datetime.now(),
            end_time=None,
            status="RUNNING",
            duration_seconds=None,
            error_message=None,
            env=cfg["env"]
        )]
        self._log(data)

    def _log_end(self, cfg, status, start_time, error_message=None):
        end_time = datetime.now()
        duration = (end_time - datetime.fromtimestamp(start_time)).total_seconds()
        data = [Row(
            transformation_id=str(cfg["id"]),
            project=cfg["project"],
            plant=cfg.get("plant"),
            asset=cfg.get("asset"),
            start_time=datetime.fromtimestamp(start_time),
            end_time=end_time,
            status=status,
            duration_seconds=duration,
            error_message=error_message,
            env=cfg["env"]
        )]
        self._log(data)

    # ----------------------------------------------------------------------
    # Core execution
    # ----------------------------------------------------------------------
    def _run_one(self, cfg, max_retries: int = 3, base_delay: float = 2.0):
        """
        Execute a single transformation with retry logic.
 
        Retries transient failures up to `max_retries` times
        before logging a permanent failure to the run log.
        
        Supports dual-engine execution with function resolution via REGISTRY.
 
        Args:
            cfg (dict): Transformation configuration record.
            max_retries (int): Number of times to retry on failure.
            base_delay (float): Base delay (seconds) for exponential backoff.
        """
        full_module = cfg['module']
        func_name = cfg["function"]
        start_time = time.time()
        
        # Determine effective engine (config override or default)
        effective_engine = cfg.get('constants', {}).get('engine', self.engine.value)
        if isinstance(effective_engine, Engine):
            effective_engine = effective_engine.value
 
        self.logger.log("info", f"Running [{cfg['project']}] {cfg.get('plant') or ''} {cfg.get('asset') or ''} ({full_module}.{func_name}) [engine={effective_engine}]")
        
        # Emit transform_start hook
        self.hooks.emit("transform_start", {
            "transformation_id": cfg.get('id'),
            "project": cfg['project'],
            "module": full_module,
            "function": func_name,
            "engine": effective_engine,
            "layer": self.layer
        })
 
        self._log_start(cfg)
 
        attempt = 0
        while attempt <= max_retries:
            try:
                # Resolve function via REGISTRY with fallback to importlib
                func = REGISTRY.resolve(full_module, func_name, effective_engine)
                
                if func is None:
                    # Fallback to importlib for legacy functions
                    module = importlib.import_module(full_module)
                    func = getattr(module, func_name)
                
                # Build ExecutionContext
                context = ExecutionContext(
                    engine=Engine.SPARK if effective_engine == "spark" else Engine.PANDAS,
                    project=cfg['project'],
                    env=cfg.get('env', self.env),
                    spark=self.spark,
                    sql_provider=self.sql_provider,
                    logger=self.logger,
                    hooks=self.hooks,
                    extras={
                        "plant": cfg.get('plant'),
                        "asset": cfg.get('asset'),
                        "layer": self.layer
                    }
                )
                
                # Merge config data with kwargs (only pass non-None values)
                func_kwargs = {**self.kwargs}
                
                # Only add these if they have meaningful values
                if cfg.get('inputs'):
                    func_kwargs['inputs'] = cfg.get('inputs', [])
                if cfg.get('constants'):
                    func_kwargs['constants'] = cfg.get('constants', {})
                if cfg.get('outputs'):
                    func_kwargs['outputs'] = cfg.get('outputs', [])
                if self.spark:
                    func_kwargs['spark'] = self.spark
                if cfg.get('env'):
                    func_kwargs['env'] = cfg.get('env', self.env)
                
                # Conditionally pass context if function accepts it
                sig = inspect.signature(func)
                if 'context' in sig.parameters:
                    func_kwargs['context'] = context
                
                func(**func_kwargs)
 
                # success path
                self._log_end(cfg, "SUCCESS", start_time)
                self.logger.log("info", f"Success: {full_module}.{func_name}")
                
                # Emit transform_success hook
                self.hooks.emit("transform_success", {
                    "transformation_id": cfg.get('id'),
                    "project": cfg['project'],
                    "module": full_module,
                    "function": func_name,
                    "duration_seconds": time.time() - start_time
                })
                
                return  # exit after success
 
            except Exception as e:
                attempt += 1
                
                # Emit transform_retry hook
                if attempt <= max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    self.logger.log("warning", f"Retry {attempt}/{max_retries} for {full_module}.{func_name} in {delay:.1f}s | {e}")
                    
                    self.hooks.emit("transform_retry", {
                        "transformation_id": cfg.get('id'),
                        "project": cfg['project'],
                        "module": full_module,
                        "function": func_name,
                        "attempt": attempt,
                        "max_retries": max_retries,
                        "error": str(e)
                    })
                    
                    time.sleep(delay)
                else:
                    # all retries failed
                    self._log_end(cfg, "FAILED", start_time, str(e).replace("'", ""))
                    self.logger.log("error", f"Failed after {max_retries} retries: {full_module}.{func_name} | {e}")
                    
                    # Emit transform_failure hook
                    self.hooks.emit("transform_failure", {
                        "transformation_id": cfg.get('id'),
                        "project": cfg['project'],
                        "module": full_module,
                        "function": func_name,
                        "error": str(e),
                        "duration_seconds": time.time() - start_time
                    })
                    
                    return


    def run_all(self):
        """Runs all transformations sequentially"""
        configs = self._fetch_configs()
        if not configs:
            self.logger.log("warning", f"No enabled transformations found for {self.project} ({self.env})")
            return
        
        # Emit configs_loaded hook
        self.hooks.emit("configs_loaded", {
            "project": self.project,
            "env": self.env,
            "layer": self.layer,
            "count": len(configs)
        })
        
        for cfg in configs:
            self._run_one(cfg)

    def run_parallel(self):
        """Runs all transformations in parallel using ThreadPoolExecutor"""
        configs = self._fetch_configs()
        if not configs:
            self.logger.log("warning", f"No enabled transformations found for {self.project} ({self.env})")
            return
        
        # Emit configs_loaded hook
        self.hooks.emit("configs_loaded", {
            "project": self.project,
            "env": self.env,
            "layer": self.layer,
            "count": len(configs)
        })

        self.logger.log("info", f"Running {len(configs)} transformations in parallel (max_workers={self.max_workers})")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._run_one, cfg): cfg for cfg in configs}
            for future in as_completed(futures):
                cfg = futures[future]
                try:
                    future.result()
                except Exception as e:
                    import traceback
                    self.logger.log("error", f"Parallel task failed for {cfg['module']}.{cfg.get('function')} | {e}")
                    print("Full traceback:")
                    traceback.print_exc()
                    print("-" * 80)
