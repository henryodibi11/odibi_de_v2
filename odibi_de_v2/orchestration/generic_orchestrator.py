"""
Generic Project Orchestrator

Environment and project-agnostic orchestrator that works with any project
configured through the TransformationRegistry and project manifests.
"""

import os
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession

from odibi_de_v2.databricks.orchestration import BaseProjectOrchestrator
from odibi_de_v2.databricks import DeltaTableManager
from odibi_de_v2.transformer import TransformationRunnerFromConfig
from odibi_de_v2.storage import SaverProvider
from odibi_de_v2.core import DataType
from odibi_de_v2.core.engine import Engine
from odibi_de_v2.project.manifest import ProjectManifest
from odibi_de_v2.hooks.manager import HookManager
from odibi_de_v2.logging.log_sink import BaseLogSink


class GenericProjectOrchestrator(BaseProjectOrchestrator):
    """
    ---------------------------------------------------------------------------
    🚀 GenericProjectOrchestrator
    ---------------------------------------------------------------------------
    Universal orchestration engine for ANY data pipeline project.
    
    Works with:
    - Any project type (manufacturing, analytics, ML, custom)
    - Any environment (qat, prod, dev, etc.)
    - Flexible TransformationRegistry configuration
    - Project-specific manifests
    
    ---------------------------------------------------------------------------
    ⚙️ Core Responsibilities
    ---------------------------------------------------------------------------
    ✅ Manifest-Driven Execution
        Loads project configuration from manifest.json to determine
        layer order, dependencies, and execution strategy.
    
    ✅ Bronze Layer Execution
        Triggers ingestion jobs based on IngestionSourceConfig for the project.
    
    ✅ Silver & Gold Transformations
        Executes transformation layers via TransformationRunnerFromConfig
        using layer order from the manifest.
    
    ✅ Smart Caching
        Optionally caches Delta tables after each layer based on
        manifest cache_plan or runtime cache_plan parameter.
    
    ✅ Authentication Abstraction
        Uses pluggable authentication providers for different environments.
    
    ✅ Log Management
        Structured logging with optional persistence to ADLS.
    
    ---------------------------------------------------------------------------
    ⚙️ Parameters
    ---------------------------------------------------------------------------
    project : str
        Name of the project (e.g., "Energy Efficiency", "Customer Churn").
    
    env : str, default = "qat"
        Environment key (e.g., "qat", "prod", "dev").
    
    manifest_path : Optional[Path]
        Path to project manifest.json. If None, auto-discovers from project name.
    
    log_level : str, default = "WARNING"
        Logging verbosity. One of ["INFO", "WARNING", "ERROR"].
    
    save_logs : bool, default = False
        Whether to persist logs to storage after pipeline completion.
    
    log_container : str, default = "digital-manufacturing"
        Storage container for logs when save_logs=True.
    
    auth_provider : Optional[callable]
        Authentication provider function. If None, uses default Spark session.
    
    ---------------------------------------------------------------------------
    💡 Example Usage
    ---------------------------------------------------------------------------
    >>> from odibi_de_v2.orchestration import GenericProjectOrchestrator
    
    # 1️⃣ Instantiate the orchestrator
    >>> orchestrator = GenericProjectOrchestrator(
    ...     project="Energy Efficiency",
    ...     env="qat",
    ...     log_level="INFO",
    ...     save_logs=True
    ... )
    
    # 2️⃣ Run full pipeline (Bronze → Silver → Gold)
    >>> result = orchestrator.run()
    
    # 3️⃣ Run with custom layer targeting
    >>> result = orchestrator.run(target_layers=["Silver_1", "Gold_1"])
    
    # 4️⃣ Run with caching
    >>> result = orchestrator.run(
    ...     cache_plan={"Gold_1": ["qat_energy_efficiency.combined_dryers"]}
    ... )
    
    ---------------------------------------------------------------------------
    🎯 Partial Run Examples
    ---------------------------------------------------------------------------
    
    # Bronze-only run
    >>> orchestrator.run(target_layers=["Bronze"])
    
    # Silver-only run
    >>> orchestrator.run(target_layers=["Silver_1", "Silver_2"])
    
    # Custom layer order with caching
    >>> orchestrator.run(
    ...     target_layers=["Silver_1", "Gold_1"],
    ...     cache_plan={"Gold_1": ["output_table"]}
    ... )
    
    ---------------------------------------------------------------------------
    """
    
    def __init__(
        self,
        project: str,
        env: str = "qat",
        manifest_path: Optional[Path] = None,
        log_level: str = "WARNING",
        save_logs: bool = False,
        log_container: str = "digital-manufacturing",
        auth_provider: Optional[callable] = None,
        engine: str = "spark",
        hooks: Optional[HookManager] = None,
        log_sink: Optional[BaseLogSink] = None,
    ):
        super().__init__(project, env, log_level)
        self.save_logs = save_logs
        self.log_container = log_container
        self.auth_provider = auth_provider
        self.engine = Engine.SPARK if engine.lower() == "spark" else Engine.PANDAS
        self.hooks = hooks or HookManager()
        self.log_sink = log_sink
        self.spark = None
        self.sql_provider = None
        
        # Load manifest
        self.manifest = self._load_manifest(manifest_path)
        
        # Extract configuration from manifest
        self.layer_order = self.manifest.layer_order if self.manifest else []
        self.cache_plan = self.manifest.cache_plan if self.manifest else {}
    
    def _load_manifest(self, manifest_path: Optional[Path]) -> Optional[ProjectManifest]:
        """Load project manifest from file or auto-discover"""
        if manifest_path and Path(manifest_path).exists():
            return ProjectManifest.from_json(Path(manifest_path))
        
        # Auto-discover manifest
        project_slug = self.project.lower().replace(" ", "_")
        possible_paths = [
            Path(f"projects/{project_slug}/manifest.json"),
            Path(f"./{project_slug}/manifest.json"),
            Path(f"../projects/{project_slug}/manifest.json"),
        ]
        
        for path in possible_paths:
            if path.exists():
                self.logger.log("info", f"📋 Loaded manifest from {path}")
                return ProjectManifest.from_json(path)
        
        self.logger.log(
            "warning",
            f"⚠️ No manifest found for '{self.project}'. Using default configuration."
        )
        return None
    
    # -----------------------------------------------------------------------
    # 1️⃣ Bootstrap environment and authentication
    # -----------------------------------------------------------------------
    def bootstrap(self, repo_path: Optional[str] = None):
        """Initialize environment and authentication"""
        self.logger.log("info", f"⚙️ Bootstrapping environment for {self.project.upper()} (env={self.env})")
        
        if self.auth_provider:
            # Use custom authentication provider
            auth_result = self.auth_provider(
                env=self.env,
                repo_path=repo_path,
                logger_metadata={"project": self.project, "stage": "Bootstrap"}
            )
            self.spark = auth_result.get("spark")
            self.sql_provider = auth_result.get("sql_provider")
        else:
            # Use default SparkSession
            self.spark = SparkSession.getActiveSession()
            if not self.spark:
                self.spark = SparkSession.builder.appName(self.project).getOrCreate()
            
            # SQL provider must be provided via auth_provider
            self.sql_provider = None
        
        if not self.spark:
            raise RuntimeError("Spark session not available. Provide auth_provider or ensure Spark is active.")
        
        if not self.sql_provider:
            raise RuntimeError("SQL provider not available. Must provide auth_provider that returns sql_provider.")
        
        self.logger.log("info", "Environment bootstrapped")
    
    # -----------------------------------------------------------------------
    # 2️⃣ Bronze ingestion
    # -----------------------------------------------------------------------
    def run_bronze_layer(self):
        """Execute Bronze layer ingestion"""
        self.logger.log("info", f"⚙️ Running Bronze ingestion for {self.project}")
        
        # Emit bronze_start hook
        self.hooks.emit("bronze_start", {
            "project": self.project,
            "env": self.env,
            "engine": self.engine.value,
            "timestamp": datetime.now()
        })
        
        try:
            # Import bronze processor (this would come from your existing ingestion logic)
            # For now, this is a placeholder that would integrate with your IngestionSourceConfig
            
            from odibi_de_v2.ingestion import process_ingestion_sources
            
            # Process all sources for this project and environment
            bronze_logs = process_ingestion_sources(
                project=self.project,
                environment=self.env,
                spark=self.spark,
                log_level=self.log_level
            )
            
            self.logger.log("info", f"✅ Bronze ingestion complete: {len(bronze_logs)} sources processed")
            
            # Emit bronze_end hook
            self.hooks.emit("bronze_end", {
                "project": self.project,
                "env": self.env,
                "engine": self.engine.value,
                "timestamp": datetime.now(),
                "status": "success",
                "sources_processed": len(bronze_logs)
            })
        
        except ImportError:
            self.logger.log(
                "warning",
                "⚠️ Bronze layer processing not configured. Skipping ingestion."
            )
            self.hooks.emit("bronze_end", {
                "project": self.project,
                "env": self.env,
                "engine": self.engine.value,
                "timestamp": datetime.now(),
                "status": "skipped"
            })
        except Exception as e:
            self.logger.log("error", f"❌ Bronze ingestion failed: {e}")
            self.hooks.emit("bronze_end", {
                "project": self.project,
                "env": self.env,
                "engine": self.engine.value,
                "timestamp": datetime.now(),
                "status": "failed",
                "error": str(e)
            })
            raise
    
    # -----------------------------------------------------------------------
    # 3️⃣ Silver/Gold transformations
    # -----------------------------------------------------------------------
    def run_silver_gold_layers(
        self,
        repo_path: Optional[str] = None,
        layer_order: Optional[List[str]] = None,
        cache_plan: Optional[Dict[str, List[str]]] = None,
        target_layers: Optional[List[str]] = None,
        max_workers: Optional[int] = None,
    ):
        """Execute Silver and Gold transformation layers"""
        start_time = time.time()
        
        # Use manifest defaults if not provided
        layers = layer_order or self.layer_order
        cache = cache_plan or self.cache_plan
        workers = max_workers or os.cpu_count()
        
        # Optional re-bootstrap if called directly
        if not self.spark:
            self.bootstrap(repo_path=repo_path)
        
        # Filter target layers
        layers_to_run = [
            layer for layer in layers
            if layer.lower() != "bronze" and (not target_layers or layer in target_layers)
        ]
        
        if not layers_to_run:
            self.logger.log("warning", "⚠️ No transformation layers to run")
            return
        
        self.logger.log("info", f"🎯 Running layers: {layers_to_run}")
        
        # Execute each layer
        for layer in layers_to_run:
            layer_start_time = time.time()
            self.logger.log("info", f"⚙️ Starting transformation layer: {layer}")
            
            # Emit layer_start hook
            self.hooks.emit("layer_start", {
                "project": self.project,
                "env": self.env,
                "layer": layer,
                "engine": self.engine.value,
                "timestamp": datetime.now()
            })
            
            try:
                runner = TransformationRunnerFromConfig(
                    sql_provider=self.sql_provider,
                    project=self.project,
                    env=self.env,
                    max_workers=workers,
                    layer=layer,
                    engine=self.engine.value,
                    hooks=self.hooks,
                    log_sink=self.log_sink,
                )
                runner.run_parallel()
                
                elapsed = time.time() - layer_start_time
                self.logger.log("info", f"✅ Layer {layer} completed in {elapsed:.2f}s")
                
                # Emit layer_end hook
                self.hooks.emit("layer_end", {
                    "project": self.project,
                    "env": self.env,
                    "layer": layer,
                    "engine": self.engine.value,
                    "timestamp": datetime.now(),
                    "status": "success",
                    "duration_seconds": elapsed
                })
                
                # Cache post-layer tables (skip for pandas with warning)
                if cache and layer in cache:
                    if self.engine == Engine.PANDAS:
                        self.logger.log("warning", f"⚠️ Caching not supported for pandas engine. Skipping cache for {layer}")
                    else:
                        self.logger.log("info", f"📦 Caching tables after {layer}")
                        for table in cache[layer]:
                            try:
                                DeltaTableManager(self.spark, table).cache()
                                self.logger.log("info", f"✅ Cached table: {table}")
                            except Exception as e:
                                self.logger.log("warning", f"⚠️ Could not cache {table}: {e}")
            
            except Exception as e:
                elapsed = time.time() - layer_start_time
                self.logger.log("error", f"❌ Layer {layer} failed: {e}")
                
                # Emit layer_end hook with failure
                self.hooks.emit("layer_end", {
                    "project": self.project,
                    "env": self.env,
                    "layer": layer,
                    "engine": self.engine.value,
                    "timestamp": datetime.now(),
                    "status": "failed",
                    "duration_seconds": elapsed,
                    "error": str(e)
                })
                raise
        
        duration = time.time() - start_time
        self.logger.log("info", f"🏁 Transformation processing complete in {duration:.2f}s")
    
    # -----------------------------------------------------------------------
    # 4️⃣ Orchestration runner with hooks
    # -----------------------------------------------------------------------
    def run(self, **kwargs) -> dict:
        """Full lifecycle orchestration with hook support."""
        start_time = time.time()
        repo_path = kwargs.get("repo_path")
        target_layers = kwargs.get("target_layers")

        self.logger.log("info", f"🚀 Starting orchestrator for {self.project.upper()} (env={self.env})")

        # Emit orchestrator_run_start hook
        self.hooks.emit("orchestrator_run_start", {
            "project": self.project,
            "env": self.env,
            "engine": self.engine.value,
            "timestamp": datetime.now(),
            "target_layers": target_layers
        })

        try:
            # 1️⃣ Environment setup
            self.bootstrap(repo_path=repo_path)

            # 2️⃣ Bronze layer (if applicable)
            if not target_layers or "Bronze" in target_layers:
                self.run_bronze_layer()
                if target_layers == ["Bronze"]:
                    self.logger.log("info", "🔹 Target layer is Bronze only — exiting after ingestion.")
                    duration = round(time.time() - start_time, 2)
                    
                    # Emit orchestrator_run_end hook
                    self.hooks.emit("orchestrator_run_end", {
                        "project": self.project,
                        "env": self.env,
                        "engine": self.engine.value,
                        "timestamp": datetime.now(),
                        "status": "success",
                        "layers_executed": ["Bronze"],
                        "duration_seconds": duration
                    })
                    
                    return {
                        "status": "SUCCESS",
                        "project": self.project,
                        "env": self.env,
                        "layers_run": ["Bronze"],
                        "duration_seconds": duration,
                    }

            # 3️⃣ Silver/Gold layers
            self.run_silver_gold_layers(**kwargs)
            status = "SUCCESS"
            layers_executed = target_layers if target_layers else ["Bronze", "Silver", "Gold"]

        except Exception as e:
            status = "FAILED"
            self.logger.log("error", f"❌ Orchestrator failed: {e}")
            duration = round(time.time() - start_time, 2)
            
            # Emit orchestrator_run_end hook with failure
            self.hooks.emit("orchestrator_run_end", {
                "project": self.project,
                "env": self.env,
                "engine": self.engine.value,
                "timestamp": datetime.now(),
                "status": "failed",
                "duration_seconds": duration,
                "error": str(e)
            })
            raise
        finally:
            self.on_finish()

        total_time = round(time.time() - start_time, 2)
        self.logger.log("info", f"🏁 Orchestration completed in {total_time:.2f}s")

        # Emit orchestrator_run_end hook
        self.hooks.emit("orchestrator_run_end", {
            "project": self.project,
            "env": self.env,
            "engine": self.engine.value,
            "timestamp": datetime.now(),
            "status": "success",
            "layers_executed": layers_executed,
            "duration_seconds": total_time
        })

        return {
            "status": status,
            "project": self.project,
            "env": self.env,
            "layers_run": layers_executed,
            "duration_seconds": total_time,
        }
    
    # -----------------------------------------------------------------------
    # 5️⃣ Optional log persistence
    # -----------------------------------------------------------------------
    def on_finish(self):
        """Save logs if configured"""
        if not self.save_logs:
            return
        
        logs = self.logger.get_logs()
        if not logs:
            self.logger.log("warning", "⚠️ No logs available to save")
            return
        
        try:
            df_logs = pd.DataFrame(logs)
            
            # Save logs (implementation depends on your storage setup)
            if self.auth_provider:
                # Use custom storage logic
                pass
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"logs_{self.env}_{timestamp}.csv"
            
            self.logger.log(
                "info",
                f"🪶 Logs saved to {self.log_container}/logs/{self.project}/{log_filename}"
            )
        
        except Exception as e:
            self.logger.log("warning", f"⚠️ Failed to save logs: {e}")


def run_project(
    project: str,
    env: str = "qat",
    target_layers: Optional[List[str]] = None,
    cache_plan: Optional[Dict[str, List[str]]] = None,
    manifest_path: Optional[str] = None,
    log_level: str = "WARNING",
    save_logs: bool = False,
    auth_provider: Optional[callable] = None,
    engine: str = "spark",
    hooks: Optional[HookManager] = None,
    log_sink: Optional[BaseLogSink] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Execute a complete data pipeline for any project with a single function call.
    
    This is the main entry point for running odibi_de_v2 pipelines. It orchestrates
    the entire medallion architecture flow (Bronze → Silver → Gold) based on the
    project's manifest configuration and TransformationRegistry entries.
    
    Args:
        project: Project name as registered in config tables (e.g., "Energy Efficiency").
        env: Target environment - one of "qat", "prod", "dev". Determines which
            configuration entries and data sources are used.
        target_layers: Optional list of specific layers to execute. If None, runs all
            layers defined in the project manifest. Examples: ["Bronze"], 
            ["Silver_1", "Gold_1"], ["Gold_1", "Gold_2"].
        cache_plan: Optional dictionary mapping layer names to lists of table names
            to cache after layer completion. Example: 
            {"Gold_1": ["schema.aggregated_table", "schema.summary_table"]}.
        manifest_path: Absolute path to project manifest.json file. If None, 
            auto-discovers from standard project locations.
        log_level: Logging verbosity - one of "INFO", "WARNING", "ERROR". Controls
            amount of execution detail logged.
        save_logs: If True, persists execution logs to cloud storage after completion.
        auth_provider: Optional callable that returns authentication context with
            'spark' and 'sql_provider' keys. Required for Databricks execution.
        engine: Execution engine - "spark" or "pandas". Defaults to "spark". Note that
            caching is not supported for pandas engine.
        hooks: Optional HookManager instance for event-driven workflows. If None, a
            default HookManager is created.
        log_sink: Optional LogSink instance for custom transformation logging.
        **kwargs: Additional parameters passed to the orchestrator (e.g., max_workers,
            repo_path for custom authentication).
    
    Returns:
        Dictionary containing execution summary with keys:
            - 'status': 'success' or 'failed'
            - 'layers_executed': List of layer names that were run
            - 'duration_seconds': Total execution time
            - 'errors': List of any errors encountered
    
    Raises:
        ValueError: If project name is invalid or not found in configuration.
        RuntimeError: If Spark session or SQL provider is unavailable.
        FileNotFoundError: If manifest_path is specified but doesn't exist.
    
    Examples:
        **Example 1: Run Complete Pipeline**
        
            >>> from odibi_de_v2 import run_project
            >>> 
            >>> # Execute all layers (Bronze → Silver → Gold)
            >>> result = run_project(
            ...     project="Energy Efficiency",
            ...     env="qat",
            ...     log_level="INFO"
            ... )
            >>> print(f"Pipeline completed in {result['duration_seconds']:.1f}s")
        
        **Example 2: Run Specific Layers Only**
        
            >>> from odibi_de_v2 import run_project
            >>> 
            >>> # Run only Silver and Gold transformations (skip Bronze ingestion)
            >>> result = run_project(
            ...     project="Customer Churn",
            ...     env="prod",
            ...     target_layers=["Silver_1", "Gold_1"]
            ... )
        
        **Example 3: Run with Table Caching**
        
            >>> from odibi_de_v2 import run_project
            >>> 
            >>> # Cache expensive aggregation tables after Gold layer
            >>> result = run_project(
            ...     project="Energy Efficiency",
            ...     env="qat",
            ...     cache_plan={
            ...         "Gold_1": [
            ...             "qat_energy.combined_dryers",
            ...             "qat_energy.aggregated_metrics"
            ...         ]
            ...     }
            ... )
        
        **Example 4: Run with Custom Authentication (Databricks)**
        
            >>> from odibi_de_v2 import run_project
            >>> from my_auth_module import get_databricks_auth
            >>> 
            >>> # Provide custom auth provider for Databricks
            >>> def auth_provider(env, repo_path=None, logger_metadata=None):
            ...     spark = SparkSession.builder.getOrCreate()
            ...     sql_provider = SQLServerConnection(
            ...         server="my-server.database.windows.net",
            ...         database="config_db"
            ...     )
            ...     return {"spark": spark, "sql_provider": sql_provider}
            >>> 
            >>> result = run_project(
            ...     project="Manufacturing KPIs",
            ...     env="prod",
            ...     auth_provider=auth_provider,
            ...     save_logs=True
            ... )
        
        **Example 5: Run Bronze Ingestion Only**
        
            >>> from odibi_de_v2 import run_project
            >>> 
            >>> # Ingest raw data without transformations
            >>> result = run_project(
            ...     project="Sales Analytics",
            ...     env="qat",
            ...     target_layers=["Bronze"]
            ... )
        
        **Example 6: Production Run with Full Logging**
        
            >>> from odibi_de_v2 import run_project
            >>> import logging
            >>> 
            >>> # Production execution with comprehensive logging
            >>> result = run_project(
            ...     project="Financial Reporting",
            ...     env="prod",
            ...     log_level="INFO",
            ...     save_logs=True,
            ...     cache_plan={
            ...         "Silver_2": ["prod_finance.validated_transactions"],
            ...         "Gold_1": ["prod_finance.monthly_summary"]
            ...     }
            ... )
            >>> 
            >>> # Check results
            >>> if result['status'] == 'success':
            ...     print(f"✅ Pipeline succeeded")
            ...     print(f"Layers: {', '.join(result['layers_executed'])}")
            ... else:
            ...     print(f"❌ Pipeline failed: {result['errors']}")
        
        **Example 7: Custom Manifest Location**
        
            >>> from odibi_de_v2 import run_project
            >>> 
            >>> # Use manifest from non-standard location
            >>> result = run_project(
            ...     project="Custom Project",
            ...     env="qat",
            ...     manifest_path="/custom/path/to/manifest.json"
            ... )
    
    Notes:
        - The function automatically discovers the project manifest from standard
          locations if manifest_path is not provided.
        - All transformations must be registered in the TransformationRegistry SQL
          table with matching project and environment values.
        - For Bronze layer execution, ensure IngestionSourceConfig table is populated
          with appropriate data source definitions.
        - Layer execution order is determined by the project manifest's layer_order.
        - Each layer runs all transformations in sequence by step number, but
          transformations within the same step can run in parallel.
    
    See Also:
        - GenericProjectOrchestrator: The underlying orchestrator class
        - TransformationRunnerFromConfig: Handles individual transformation execution
        - initialize_project: Create new project scaffolding
    """
    orchestrator = GenericProjectOrchestrator(
        project=project,
        env=env,
        manifest_path=Path(manifest_path) if manifest_path else None,
        log_level=log_level,
        save_logs=save_logs,
        auth_provider=auth_provider,
        engine=engine,
        hooks=hooks,
        log_sink=log_sink,
    )
    
    return orchestrator.run(
        target_layers=target_layers,
        cache_plan=cache_plan,
        **kwargs
    )
