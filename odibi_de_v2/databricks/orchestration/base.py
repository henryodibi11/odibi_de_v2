from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from odibi_de_v2.logger import get_logger
import os

class BaseProjectOrchestrator(ABC):
    """
    ---------------------------------------------------------------------------
    🧩 BaseProjectOrchestrator (Abstract Contract)
    ---------------------------------------------------------------------------
    Defines a reusable lifecycle contract for orchestrating Databricks-based
    project pipelines (Bronze → Silver → Gold). Concrete subclasses handle
    environment setup, ingestion, and transformations.
    ---------------------------------------------------------------------------
    """

    def __init__(self, project: str, env: str = "qat", log_level: str = "INFO"):
        self.project = project
        self.env = env
        self.logger = get_logger()

    # --- Required lifecycle hooks ---
    @abstractmethod
    def bootstrap(self, repo_path: Optional[str] = None) -> None:
        """Perform environment setup (authenticators, imports, etc.)."""
        pass

    @abstractmethod
    def run_bronze_layer(self) -> None:
        """Handle raw ingestion or base-level data preparation."""
        pass

    @abstractmethod
    def run_silver_gold_layers(
        self,
        layer_order: Optional[list[str]] = None,
        cache_plan: Optional[dict[str, list[str]]] = None,
        target_layers: Optional[list[str]] = None,
        max_workers: int = os.cpu_count()
    ) -> None:
        """Execute Silver/Gold transformation layers."""
        pass

    # --- Optional hooks ---
    def on_start(self) -> None:
        self.logger.info(f"Starting orchestrator for {self.project} [{self.env}]")

    def on_finish(self) -> None:
        self.logger.info(f"Finished orchestrator for {self.project} [{self.env}]")

    def on_error(self, exc: Exception) -> None:
        self.logger.error(f"Error during orchestration: {exc}", exc_info=True)

    # --- Unified entrypoint ---
    def run(self, **kwargs) -> dict[str, object]:
        """Execute the full orchestration lifecycle."""
        import time
        start = time.time()
        self.on_start()
        try:
            self.bootstrap(kwargs.get("repo_path"))
            if "Bronze" in (kwargs.get("target_layers") or ["Bronze"]):
                self.run_bronze_layer()
            self.run_silver_gold_layers(**kwargs)
            status = "SUCCESS"
        except Exception as exc:
            status = "FAILED"
            self.on_error(exc)
        finally:
            self.on_finish()
        return {
            "project": self.project,
            "env": self.env,
            "status": status,
            "duration_seconds": round(time.time() - start, 2),
        }
