from odibi_de_v2.databricks import DeltaTableManager
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DoubleType
)
import hashlib, json, uuid, datetime
from typing import Optional, Dict
from odibi_de_v2.logger import log_and_optionally_raise


class TransformationTracker:
    """
    Logs DataFrame transformations to a Delta table for lineage, performance, and quality.
    Same behavior as before, with safer hashing, optional counts, and micro-optimizations.
    """

    def __init__(
        self,
        table_path: str,
        table_name: Optional[str] = None,
        database: Optional[str] = None,
        project: Optional[str] = None,
        auto_register: bool = True
    ):
        """
        Args:
            table_path (str): Delta table storage path (ADLS path).
            table_name (str): Logical name to register in the metastore.
            database (str): Database name to register under (optional but recommended).
            project (str): Project name to register under (optional but recommended).
            auto_register (bool): Whether to register in the metastore automatically.
        """
        self.table_path = table_path
        self.table_name = table_name
        self.database = database
        self.project = project
        self.auto_register = auto_register

        self.spark = SparkSession.getActiveSession()
        self.run_id = None
        self._schema_cache = {}

        self._ensure_table_exists()

    # ----------------------------------------------------------------------
    def new_run(self):
        """Generate a new run_id each workflow execution."""
        self.run_id = str(uuid.uuid4())
        msg = f"[TransformationTracker] ▶️  New run started: {self.run_id}"
        print(msg)
        log_and_optionally_raise(
            module="Transformer",
            component="TransformationTracker",
            method="new_run",
            message=msg,
            level="INFO"
        )
    # ----------------------------------------------------------------------
    def _ensure_table_exists(self):
        """Ensure the Delta table exists and is registered if requested."""
        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("run_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("node_name", StringType(), True),
            StructField("parent_node", StringType(), True),
            StructField("step_type", StringType(), True),
            StructField("intent", StringType(), True),
            StructField("input_row_count", LongType(), True),
            StructField("output_row_count", LongType(), True),
            StructField("input_schema_hash", StringType(), True),
            StructField("output_schema_hash", StringType(), True),
            StructField("schema_change_summary", StringType(), True),
            StructField("step_order", LongType(), True),
            StructField("output_schema_json", StringType(), True),
            StructField("status", StringType(), True),
            StructField("duration_seconds", DoubleType(), True)
        ])

        try:
            self.spark.read.format("delta").load(self.table_path)
        except Exception:
            self.spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(self.table_path)
            msg = f"[TransformationTracker] ✅ Created Delta table at {self.table_path}"
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationTracker",
                method="_ensure_table_exists",
                message=msg,
                level="INFO"
            )
        # Auto-register for SQL querying
        if self.auto_register and self.database:
            try:
                dtm = DeltaTableManager(
                    spark=self.spark,
                    table_or_path=self.table_path,
                    is_path=True
                )
                table_name=f"{self.project}_{self.table_name}".strip().lower().replace(" ", "_")
                dtm.register_table(
                    table_name=table_name,
                    database=self.database
                )
                msg = f"[TransformationTracker] 📘 Registered table as {self.database}.{table_name}"
                log_and_optionally_raise(
                    module="Transformer",
                    component="TransformationTracker",
                    method="_ensure_table_exists",
                    message=msg,
                    level="INFO"
                )
            except Exception as e:
                msg = f"[TransformationTracker] ⚠️ Failed to register table: {e}"
                log_and_optionally_raise(
                    module="Transformer",
                    component="TransformationTracker",
                    method="_ensure_table_exists",
                    message=msg,
                    level="ERROR"
                )
    # ----------------------------------------------------------------------
    def _hash_schema(self, df: Optional[DataFrame]) -> Optional[str]:
        """Cache schema hashes to avoid recomputing identical ones."""
        if df is None:
            return None
        schema_json = json.dumps(df.schema.jsonValue(), sort_keys=True)
        if schema_json not in self._schema_cache:
            self._schema_cache[schema_json] = hashlib.sha256(schema_json.encode()).hexdigest()
        return self._schema_cache[schema_json]

    def _schema_diff(self, before_df: Optional[DataFrame], after_df: DataFrame) -> Dict[str, list]:
        """Return added/removed columns for quick drift insight."""
        if before_df is None:
            return {"added": [f.name for f in after_df.schema.fields], "removed": []}
        before_cols = set(f"{f.name}:{f.dataType}" for f in before_df.schema.fields)
        after_cols = set(f"{f.name}:{f.dataType}" for f in after_df.schema.fields)
        return {
            "added": list(after_cols - before_cols),
            "removed": list(before_cols - after_cols)
        }

    # ----------------------------------------------------------------------
    def log(
        self,
        before_df: Optional[DataFrame],
        after_df: DataFrame,
        node_name: str,
        step_type: str,
        intent: Optional[str] = None,
        parent_node: Optional[str] = None,
        layer: Optional[str] = None,
        step_order: Optional[int] = None,
        status: str = "SUCCESS",
        duration_seconds: Optional[float] = None,
        compute_counts: bool = False,
    ):
        """Write a single transformation record to Delta."""
        spark = self.spark
        timestamp = datetime.datetime.now()

        # ✅ Optional counts (skip heavy actions unless requested)
        in_rows = before_df.count() if compute_counts and before_df is not None else None
        out_rows = after_df.count() if compute_counts else None

        diff_summary = json.dumps(self._schema_diff(before_df, after_df))
        in_schema_hash = self._hash_schema(before_df)
        out_schema_hash = self._hash_schema(after_df)
        out_schema_json = json.dumps(after_df.schema.jsonValue())

        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("run_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("node_name", StringType(), True),
            StructField("parent_node", StringType(), True),
            StructField("step_type", StringType(), True),
            StructField("intent", StringType(), True),
            StructField("input_row_count", LongType(), True),
            StructField("output_row_count", LongType(), True),
            StructField("input_schema_hash", StringType(), True),
            StructField("output_schema_hash", StringType(), True),
            StructField("schema_change_summary", StringType(), True),
            StructField("step_order", LongType(), True),
            StructField("output_schema_json", StringType(), True),
            StructField("status", StringType(), True),
            StructField("duration_seconds", DoubleType(), True)
        ])

        record = [(timestamp, self.run_id, self.table_name, node_name, parent_node,
                   step_type, intent, in_rows, out_rows,
                   in_schema_hash, out_schema_hash, diff_summary,
                   step_order, out_schema_json, status, duration_seconds)]

        # ✅ Slightly safer write (handles small bursts reliably)
        spark.createDataFrame(record, schema=schema) \
            .write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .save(self.table_path)

    # ----------------------------------------------------------------------
    def summarize_run(self, run_id: Optional[str] = None):
        """Show high-level summary for one run."""
        df = self.spark.read.format("delta").load(self.table_path)
        if not run_id:
            run_id = df.select("run_id").orderBy(F.col("timestamp").desc()).limit(1).collect()[0][0]
        run_df = df.filter(F.col("run_id") == run_id)

        summary = run_df.agg(
            F.count("*").alias("total_steps"),
            F.sum("duration_seconds").alias("total_seconds"),
            F.sum("output_row_count").alias("total_rows_processed")
        ).collect()[0]

        msg = (
            f"🧾 Run summary for {run_id}\n"
            f"   Steps: {summary['total_steps']}\n"
            f"   Duration: {summary['total_seconds']:.2f}s\n"
            f"   Rows processed: {summary['total_rows_processed']}"
        )
        print(msg)
        log_and_optionally_raise(
            module="Transformer",
            component="TransformationTracker",
            method="summarize_run",
            message=msg,
            level="INFO"
        )
        display(run_df.orderBy("step_order"))

    # ----------------------------------------------------------------------
    def auto_schema_check(self):
        """Auto-run schema drift detection after each new run."""
        df = self.spark.read.format("delta").load(self.table_path)
        latest = df.orderBy(F.col("timestamp").desc()).select("run_id", "table_name").limit(2).collect()
        if len(latest) < 2:
            return
        run2, run1 = latest[0]["run_id"], latest[1]["run_id"]
        if self._has_drift(run1, run2):
            msg = f"⚠️  Schema drift detected between runs {run1} → {run2}"
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationTracker",
                method="auto_schema_check",
                message=msg,
                level="INFO"
            )
        else:
            msg = f"✅ No schema drift between runs {run1} and {run2}"
            print(msg)
            log_and_optionally_raise(
                module="Transformer",
                component="TransformationTracker",
                method="auto_schema_check",
                message=msg,
                level="INFO"
            )
    def _has_drift(self, run1: str, run2: str) -> bool:
        df = self.spark.read.format("delta").load(self.table_path)
        df1 = df.filter(F.col("run_id") == run1)
        df2 = df.filter(F.col("run_id") == run2)
        h1 = df1.orderBy("step_order").tail(1)[0].output_schema_hash
        h2 = df2.orderBy("step_order").tail(1)[0].output_schema_hash
        return h1 != h2

    def check_fidelity(self, run_id: Optional[str] = None, threshold: float = 0.1):
        """Check for large row-count changes between consecutive steps."""
        df = self.spark.read.format("delta").load(self.table_path)
        if not run_id:
            run_id = df.select("run_id").orderBy(F.col("timestamp").desc()).limit(1).collect()[0][0]
        run_df = df.filter(F.col("run_id") == run_id).orderBy("step_order")
        rows = run_df.select("step_order", "node_name", "output_row_count").collect()

        msg = f"🔍 Fidelity check for run {run_id}"
        print(msg)
        log_and_optionally_raise(
            module="Transformer",
            component="TransformationTracker",
            method="auto_schema_check",
            message=msg,
            level="INFO"
        )
        for i in range(1, len(rows)):
            prev, curr = rows[i - 1], rows[i]
            if prev.output_row_count and curr.output_row_count:
                delta = curr.output_row_count - prev.output_row_count
                pct = delta / max(prev.output_row_count, 1)
                flag = "⚠️" if abs(pct) > threshold else "✅"
                msg = (
                    f" {flag} Step {curr.step_order}: {prev.output_row_count} → {curr.output_row_count} \n"
                    f"({pct*100:.1f}%)")
                print(msg)
                log_and_optionally_raise(
                    module="Transformer",
                    component="TransformationTracker",
                    method="auto_schema_check",
                    message=msg,
                    level="INFO"
                )
