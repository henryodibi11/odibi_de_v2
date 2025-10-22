import time
from abc import ABC, abstractmethod
from odibi_de_v2.logger import get_logger


class BaseProjectOrchestrator(ABC):
    """
    Abstract lifecycle for project orchestration.
    Handles run timing, logging, and lifecycle flow.
    Subclasses must implement:
        • bootstrap()
        • run_bronze_layer()
        • run_silver_gold_layers()
        • on_finish()
    """

    def __init__(self, project: str, env: str, log_level: str = "WARNING"):
        self.project = project
        self.env = env
        self.logger = get_logger()
        self.logger.set_log_level(log_level)

    # -----------------------------------------------------------------------
    # Lifecycle hooks to be implemented by subclasses
    # -----------------------------------------------------------------------
    @abstractmethod
    def bootstrap(self, repo_path=None):
        pass

    @abstractmethod
    def run_bronze_layer(self):
        pass

    @abstractmethod
    def run_silver_gold_layers(self, **kwargs):
        pass

    @abstractmethod
    def on_finish(self):
        pass

    # -----------------------------------------------------------------------
    # Orchestration runner
    # -----------------------------------------------------------------------
    def run(self, **kwargs) -> dict:
        """Full lifecycle orchestration."""
        start_time = time.time()
        repo_path = kwargs.get("repo_path")
        target_layers = kwargs.get("target_layers")

        self.logger.log("info", f"🚀 Starting orchestrator for {self.project.upper()} (env={self.env})")

        try:
            # 1️⃣ Environment setup
            self.bootstrap(repo_path=repo_path)

            # 2️⃣ Bronze layer (if applicable)
            if not target_layers or "Bronze" in target_layers:
                self.run_bronze_layer()
                if target_layers == ["Bronze"]:
                    self.logger.log("info", "🔹 Target layer is Bronze only — exiting after ingestion.")
                    return {
                        "status": "SUCCESS",
                        "project": self.project,
                        "env": self.env,
                        "layers_run": ["Bronze"],
                        "duration_seconds": round(time.time() - start_time, 2),
                    }

            # 3️⃣ Silver/Gold layers
            self.run_silver_gold_layers(**kwargs)
            status = "SUCCESS"

        except Exception as e:
            status = "FAILED"
            self.logger.log("error", f"❌ Orchestrator failed: {e}")
            raise
        finally:
            self.on_finish()

        total_time = time.time() - start_time
        self.logger.log("info", f"🏁 Orchestration completed in {total_time:.2f}s")

        return {
            "status": status,
            "project": self.project,
            "env": self.env,
            "duration_seconds": total_time,
        }
