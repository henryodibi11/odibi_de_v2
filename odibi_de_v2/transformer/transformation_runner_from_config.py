from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import time
import json
from pyspark.sql.utils import AnalysisException
from odibi_de_v2.logger import get_logger, log_and_optionally_raise
from odibi_de_v2.core import DataType,ErrorType
from odibi_de_v2.utils import get_dynamic_thread_count
import os


class TransformationRunnerFromConfig:
    """
    Global orchestrator that executes transformation functions
    defined in TransformationRegistry and logs all runs
    to config_driven.TransformationRunLog (Delta).
    
    Updated for v2.0 to support the new TransformationRegistry table
    with JSON fields for inputs, constants, and outputs.
    """
    def __init__(
        self,
        sql_provider,
        project: str,
        env: str = "qat",
        log_level: str = "ERROR",
        max_workers: int = min(32, (os.cpu_count() or 1) + 4),
        layer: str = 'Silver',
        **kwargs
        ):
        self.sql_provider = sql_provider
        self.project = project
        self.env = env
        self.log_level = log_level
        self.max_workers = max_workers
        self.spark = SparkSession.getActiveSession()
        self.log_table = "config_driven.TransformationRunLog"
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS config_driven")
        self.logger=get_logger()
        self.logger.set_log_level(log_level)
        self.layer = layer
        self.kwargs = kwargs


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
            
            print(f"DEBUG: About to query TransformationRegistry")
            print(f"DEBUG: Query: {query}")
            print(f"DEBUG: sql_provider type: {type(self.sql_provider)}")
            
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
            print(f"TransformationRegistry not found, trying legacy TransformationConfig: {e}")
            
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
        LOG_SCHEMA = StructType([
            StructField("transformation_id", StringType(), False),
            StructField("project", StringType(), False),
            StructField("plant", StringType(), True),
            StructField("asset", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("duration_seconds", FloatType(), True),
            StructField("error_message", StringType(), True),
            StructField("env", StringType(), True)
        ])
        df = self.spark.createDataFrame(data, schema=LOG_SCHEMA)
        try:
            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(self.log_table)
            )
        except AnalysisException as e:
            if "Table or view not found" in str(e):
                print(f"Creating missing Delta table: {self.log_table}")
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable(self.log_table)
                )
            else:
                raise e

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
 
        Args:
            cfg (dict): Transformation configuration record.
            max_retries (int): Number of times to retry on failure.
            base_delay (float): Base delay (seconds) for exponential backoff.
        """
        full_module = cfg['module']
        func_name = cfg["function"]
        start_time = time.time()
 
        print(f"Running [{cfg['project']}] {cfg.get('plant') or ''} {cfg.get('asset') or ''} ({full_module}.{func_name})")
        log_and_optionally_raise(
            module="Transformer",
            component="TransformationRunnerFromConfig",
            method="_run_one",
            message=f"Running [{cfg['project']}] {cfg.get('plant') or ''} {cfg.get('asset') or ''} ({full_module}.{func_name})",
            level="INFO"
        )
 
        self._log_start(cfg)
 
        attempt = 0
        while attempt <= max_retries:
            try:
                module = importlib.import_module(full_module)
                func = getattr(module, func_name)
                
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
                
                print(f"DEBUG: Calling {func_name} with kwargs keys: {list(func_kwargs.keys())}")
                
                func(**func_kwargs)
 
                # success path
                self._log_end(cfg, "SUCCESS", start_time)
                print(f"Success: {full_module}.{func_name}\n")
                log_and_optionally_raise(
                    module="Transformer",
                    component="TransformationRunnerFromConfig",
                    method="_run_one",
                    message=f"Success: {full_module}.{func_name}\n",
                    level="INFO"
                )
                return  # exit after success
 
            except Exception as e:
                attempt += 1
                if attempt <= max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    print(f"Retry {attempt}/{max_retries} for {full_module}.{func_name} in {delay:.1f}s | {e}")
                    time.sleep(delay)
                else:
                    # all retries failed
                    self._log_end(cfg, "FAILED", start_time, str(e).replace("'", ""))
                    print(f"Failed after {max_retries} retries: {full_module}.{func_name} | {e}\n")
                    log_and_optionally_raise(
                        module="Transformer",
                        component="TransformationRunnerFromConfig",
                        error_type=ErrorType.TRANSFORM_ERROR,
                        method="_run_one",
                        message=f"Failed after {max_retries} retries: {full_module}.{func_name} | {e}\n",
                        level="ERROR"
                    )
                    return


    def run_all(self):
        """Runs all transformations sequentially"""
        configs = self._fetch_configs()
        if not configs:
            print(f"No enabled transformations found for {self.project} ({self.env})")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                method="run_all",
                message=f"No enabled transformations found for {self.project} ({self.env})",
                level="WARNING"
            )
            return
        for cfg in configs:
            self._run_one(cfg)

    def run_parallel(self):
        """Runs all transformations in parallel using ThreadPoolExecutor"""
        configs = self._fetch_configs()
        if not configs:
            print(f"No enabled transformations found for {self.project} ({self.env})")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                method="run_parallel",
                message=f"No enabled transformations found for {self.project} ({self.env})",
                level="WARNING"
            )
            return

        print(f"Running {len(configs)} transformations in parallel (max_workers={self.max_workers})...")
        log_and_optionally_raise(
            module="Transformer",
            component="TransformationRunnerFromConfig",
            method="run_parallel",
            message=f"Running {len(configs)} transformations in parallel (max_workers={self.max_workers})...",
            level="INFO"
        )
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._run_one, cfg): cfg for cfg in configs}
            for future in as_completed(futures):
                cfg = futures[future]
                try:
                    future.result()
                except Exception as e:
                    import traceback
                    print(f"\nParallel task failed for {cfg['module']}.{cfg.get('function')} | {e}")
                    print("Full traceback:")
                    traceback.print_exc()
                    print("-" * 80)
                    log_and_optionally_raise(
                        module="Transformer",
                        component="TransformationRunnerFromConfig",
                        error_type=ErrorType.TRANSFORM_ERROR,
                        method="run_parallel",
                        message=f"Parallel task failed for {cfg['module']} | {e}",
                        level="ERROR")
