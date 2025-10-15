from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import time
from pyspark.sql.utils import AnalysisException
from odibi_de_v2.logger import get_logger, log_and_optionally_raise
from odibi_de_v2.core import DataType,ErrorType
from odibi_de_v2.utils import get_dynamic_thread_count

class TransformationRunnerFromConfig:
    """
    Global orchestrator that executes transformation functions
    defined in config_driven.TransformationConfig and logs all runs
    to config_driven.TransformationRunLog (Delta).
    """
    def __init__(
        self,
        sql_provider,
        project: str,
        env: str = "qat",
        log_level: str = "ERROR",
        max_workers: int = 4,
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
        return [row.asDict() for row in source_target.collect()]

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
                print(f"⚙️ Creating missing Delta table: {self.log_table}")
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
    def _run_one(self, cfg):
        """Internal method to execute a single transformation"""
        full_module = cfg['module']
        func_name = cfg["function"]
        start_time = time.time()

        print(f"🚀 Running [{cfg['project']}] {cfg.get('plant') or ''} {cfg.get('asset') or ''} ({full_module}.{func_name})")
        log_and_optionally_raise(
            module="Transformer",
            component="TransformationRunnerFromConfig",
            method="_run_one",
            message=f"🚀 Running [{cfg['project']}] {cfg.get('plant') or ''} {cfg.get('asset') or ''} ({full_module}.{func_name})",
            level="INFO"
        )
        try:
            self._log_start(cfg)
            module = importlib.import_module(full_module)
            func = getattr(module, func_name)
            func(**self.kwargs)  # or func(self.spark, **cfg)
            self._log_end(cfg, "SUCCESS", start_time)
            print(f"✅ Success: {full_module}.{func_name}\n")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                method="_run_one",
                message=f"✅ Success: {full_module}.{func_name}\n",
                level="INFO"
            )
        except Exception as e:
            self._log_end(cfg, "FAILED", start_time, str(e).replace("'", ""))
            print(f"❌ Failed: {full_module}.{func_name} | {e}\n")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                error_type=ErrorType.TRANSFORM_ERROR,
                method="_run_one",
                message=f"❌ Failed: {full_module}.{func_name} | {e}\n",
                level="ERROR")

    def run_all(self):
        """Runs all transformations sequentially"""
        configs = self._fetch_configs()
        if not configs:
            print(f"⚠️ No enabled transformations found for {self.project} ({self.env})")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                method="run_all",
                message=f"⚠️ No enabled transformations found for {self.project} ({self.env})",
                level="WARNING"
            )
            return
        for cfg in configs:
            self._run_one(cfg)

    def run_parallel(self):
        """Runs all transformations in parallel using ThreadPoolExecutor"""
        configs = self._fetch_configs()
        if not configs:
            print(f"⚠️ No enabled transformations found for {self.project} ({self.env})")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                method="run_parallel",
                message=f"⚠️ No enabled transformations found for {self.project} ({self.env})",
                level="WARNING"
            )
            return

            print(f"🧵 Running {len(configs)} transformations in parallel (max_workers={self.max_workers})...")
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationRunnerFromConfig",
                method="run_parallel",
                message=f"🧵 Running {len(configs)} transformations in parallel (max_workers={self.max_workers})...",
                level="WARNING"
            )
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._run_one, cfg): cfg for cfg in configs}
            for future in as_completed(futures):
                cfg = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Parallel task failed for {cfg['module']} | {e}")
                    log_and_optionally_raise(
                        module="Transformer",
                        component="TransformationRunnerFromConfig",
                        error_type=ErrorType.TRANSFORM_ERROR,
                        method="run_parallel",
                        message=f"❌ Parallel task failed for {cfg['module']} | {e}",
                        level="ERROR")
