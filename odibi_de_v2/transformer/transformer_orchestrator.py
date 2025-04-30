from odibi_de_v2.config import ConfigUtils
from odibi_de_v2.transformer import TransformerFromConfig, SQLGeneratorFromConfig
from odibi_de_v2.core import DataType, Framework, ErrorType
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.utils import (log_call, benchmark)
from pyspark.sql import SparkSession

import datetime
import pandas as pd
import json



class TransformerOrchestrator:
    """
    Initializes the TransformerOrchestrator with necessary configurations and Spark session.

    This orchestrator is designed to manage and execute data transformation steps based on configurations
    specified in a database table. It logs detailed audit information and optionally generates Markdown
    documentation of the executed transformation steps.

    Args:
        config_utils (ConfigUtils): Utility object to handle configurations.
        spark (SparkSession): Spark session to execute transformations.
        project (str): The project identifier used to fetch specific configurations.
        environment (str): The environment setting (e.g., 'prod', 'dev', 'qat'). Defaults to 'qat'.

    Attributes:
        config_utils (ConfigUtils): Stores the configuration utility object.
        spark (SparkSession): Spark session used for data transformations.
        project (str): Project identifier, normalized to lowercase.
        environment (str): Environment setting, normalized to lowercase.
        audit_log (list): List to store audit logs of transformation steps.
        doc_steps (list): List to store documentation steps for Markdown generation.

    Raises:
        RuntimeError: If any transformation step fails during execution, preventing further steps.

    Example:
        >>> config_util = ConfigUtils()
        >>> spark_session = SparkSession.builder.appName("Transformer").getOrCreate()
        >>> orchestrator = TransformerOrchestrator(config_util, spark_session, "sales_data", "prod")
        >>> orchestrator.run()
    """

    def __init__(self, config_utils: ConfigUtils, spark: SparkSession, project: str, environment: str = "qat"):
        """
        Initializes a new instance of the class with necessary configurations and Spark session.

        This constructor initializes the class with configurations for managing project settings and
        a Spark session for data processing. It also sets up the environment for the project, defaulting
        to 'qat' if not specified. Additionally, it initializes lists for audit logging and documenting steps.

        Args:
            config_utils (ConfigUtils): An instance of ConfigUtils for handling configuration-related operations.
            spark (SparkSession): The SparkSession object to be used for data processing.
            project (str): The name of the project. It will be converted to lowercase and stripped of leading/trailing
            whitespace.
            environment (str, optional): The environment in which the project is run. Defaults to 'qat'. It will be
            converted to lowercase and stripped of leading/trailing whitespace.

        Raises:
            ValueError: If `project` is an empty string after stripping.

        Example:
            >>> config_util = ConfigUtils()
            >>> spark_session = SparkSession.builder.appName("Example").getOrCreate()
            >>> my_class_instance = MyClass(config_util, spark_session, "Sample Project")
        """
        self.config_utils = config_utils
        self.spark = spark
        self.project = project.strip().lower()
        self.environment = environment.strip().lower()
        self.audit_log = []
        self.doc_steps = []

    @benchmark(module="CONFIG", component="TransformerOrchestrator")
    @log_call(module="CONFIG", component="TransformerOrchestrator")
    @log_exceptions(
        module="CONFIG",
        component="TransformerOrchestrator",
        error_type=ErrorType.READ_ERROR,
        raise_type=RuntimeError)
    def run(self):
        """
        Executes a series of data transformations based on configuration settings, with detailed logging
        and error handling.

        This method orchestrates the execution of data transformations for a specified project and environment.
        It reads transformation configurations, orders them, and executes each step sequentially. Execution halts
        and an exception is raised if any transformation step fails.

        Raises:
            RuntimeError: If any transformation step fails, stopping the execution of subsequent steps.

        Example:
            Assuming an instance `orchestrator` of `TransformerOrchestrator` class:
            orchestrator.run()
        """
        log_and_optionally_raise(
            module="CONFIG",
            component="TransformerOrchestrator",
            method="run",
            error_type=ErrorType.NO_ERROR,
            message=(
                f"Loading transformation config for project '{self.project}' "
                f"and environment '{self.environment}'..."),
            level="INFO")

        config_df = self.config_utils.reader_provider.read(
            data_type=DataType.SQL,
            container="",
            path_prefix="",
            object_name=self._build_transformation_query(),
            spark=self.spark
        )

        if config_df.isEmpty():
            log_and_optionally_raise(
                module="CONFIG",
                component="TransformerOrchestrator",
                method="run",
                error_type=ErrorType.NO_ERROR,
                message=(
                    "No transformations found. Exiting orchestrator."),
                level="WARNING")
            return
        log_and_optionally_raise(
            module="CONFIG",
            component="TransformerOrchestrator",
            method="run",
            error_type=ErrorType.NO_ERROR,
            message=(
                "Ordering transformations by step..."),
            level="INFO")
        config_df = config_df.orderBy("step")
        log_and_optionally_raise(
            module="CONFIG",
            component="TransformerOrchestrator",
            method="run",
            error_type=ErrorType.NO_ERROR,
            message=(
                "Starting transformations..."),
            level="INFO")
        for row in config_df.collect():
            success = self._execute_step(row)
            if not success:
                log_and_optionally_raise(
                    module="CONFIG",
                    component="TransformerOrchestrator",
                    method="run",
                    error_type=ErrorType.NO_ERROR,
                    message=(
                        "Stopping orchestrator due to step failure."),
                    level="ERROR",
                    raise_exception=True,
                    raise_type=RuntimeError)
        log_and_optionally_raise(
            module="CONFIG",
            component="TransformerOrchestrator",
            method="run",
            error_type=ErrorType.NO_ERROR,
            message=(
                "All transformations completed successfully."),
            level="INFO")

    def get_markdown(self) -> str:
        """
        Retrieves the generated Markdown documentation string for the current object.

        This method calls an internal method to generate Markdown based on the object's documentation steps.

        Returns:
            str: A string containing the generated Markdown documentation.

        Example:
            Assuming `doc_object` is an instance of a class with this method:
            markdown_string = doc_object.get_markdown()
            print(markdown_string)
            ```

        Note:
            This method does not take any arguments and relies on the internal state of the object.
        """
        return self._generate_markdown_from_doc_steps()

    def _execute_step(self, row) -> bool:
        """
        Executes a transformation step based on the provided configuration in `row`.

        This method processes a single transformation step by reading data from a source, applying a transformation,
        and writing the results to a target view. It handles both DataFrame and SQL-based transformations as specified
        in the `row`. The method logs the process and updates an audit log with the outcome of the transformation.

        Args:
            row (dict): A dictionary containing the necessary information to execute a transformation step.
            Expected keys are:
                - step (int): The step number in the transformation process.
                - source_table_or_view (str): The name of the source table or view to read data from.
                - target_view_name (str): The name of the target view to create or replace with the transformed data.
                - transformation_id (str): An identifier for the transformation configuration.
                - transformation_engine (str): The engine to use for the transformation ('dataframe' or 'sql').
                - description (str): A description of the transformation step.
                - layer (str): The data layer involved in the transformation process.

        Returns:
            bool: True if the transformation step was executed successfully, False otherwise.

        Raises:
            RuntimeError: If the source table/view does not exist or if an error occurs during the
            transformation process.
            ValueError: If an unknown transformation engine is specified.

        Example:
            Assuming a predefined method `log_and_optionally_raise` and class attributes like `spark`, `config_utils`,
            and `audit_log` are properly set up:

            row_config = {
                "step": 1,
                "source_table_or_view": "source_data",
                "target_view_name": "transformed_data",
                "transformation_id": "123",
                "transformation_engine": "dataframe",
                "description": "Initial transformation of data",
                "layer": "raw"
            }
            success = self._execute_step(row_config)
            print("Transformation successful:", success)
        """
        step_number = row["step"]
        source_table_or_view = row["source_table_or_view"].strip().lower()
        target_view_name = row["target_view_name"].strip().lower()
        transformation_id = row["transformation_id"]
        transformation_engine = row["transformation_engine"].strip().lower()
        description = row["description"]
        layer = row["layer"]
        log_and_optionally_raise(
            module="CONFIG",
            component="TransformerOrchestrator",
            method="_execute_step",
            error_type=ErrorType.NO_ERROR,
            message=(
                f"Executing Step {step_number}: {source_table_or_view} -> {target_view_name}"),
            level="INFO")

        start_time = datetime.datetime.now()
        status = "SUCCESS"
        error_message = None

        try:
            # 1. Validate source table/view exists
            if not self.spark.catalog._jcatalog.tableExists(source_table_or_view):
                raise RuntimeError(
                    f"Source table or view '{source_table_or_view}' does not exist for Step {step_number}.")
            # 2. Read source
            df = self.spark.sql(f"SELECT * FROM {source_table_or_view}").toPandas()

            # 3. Resolve config
            base_config = self.config_utils.get_config(
                query_type="transformation",
                id=transformation_id,
                project=self.project
            )

            # 4. Run correct transformer
            if transformation_engine == "dataframe":
                resolved_config = base_config["framework_transformer_config"]
                transformer = TransformerFromConfig(Framework.SPARK)
                df_transformed = self.spark.createDataFrame(
                    transformer.transform(df, config=resolved_config)
                )
            elif transformation_engine == "sql":
                resolved_config = base_config["sql_transformer_config"]
                transformer = SQLGeneratorFromConfig(config=resolved_config)
                df_transformed = self.spark.sql(transformer.generate_query())
            else:
                raise ValueError(f"Unknown transformation_engine '{transformation_engine}' in step {step_number}.")

            # 5. Register output view
            df_transformed.createOrReplaceTempView(target_view_name)
            self.doc_steps.append({
                "step": step_number,
                "transformer_type": transformation_engine,
                "description": description,
                "source_object": source_table_or_view,
                "target_object": target_view_name,
                "config": resolved_config
            })
            log_and_optionally_raise(
                module="CONFIG",
                component="TransformerOrchestrator",
                method="_execute_step",
                error_type=ErrorType.NO_ERROR,
                message=(
                    f"Step {step_number}: Transformation completed. View '{target_view_name}' created."),
                level="INFO")

        except Exception as e:
            error_message = str(e)
            status = "FAILURE"
            end_time = datetime.datetime.now()
            self.audit_log.append({
                "step_number": step_number,
                "source_object": source_table_or_view,
                "target_object": target_view_name,
                "transformation_id": transformation_id,
                "transformation_engine": transformation_engine,
                "project": self.project,
                "layer": layer,
                "environment": self.environment,
                "description": description,
                "status": status,
                "error_message": error_message,
                "start_timestamp": start_time.isoformat(),
                "end_timestamp": end_time.isoformat(),
                "duration_seconds": round((end_time - start_time).total_seconds(), 2)})

            raise RuntimeError(f"Error in Step {step_number}: {str(e)}") from e

        end_time = datetime.datetime.now()

        self.audit_log.append({
            "step_number": step_number,
            "source_object": source_table_or_view,
            "target_object": target_view_name,
            "transformation_id": transformation_id,
            "transformation_engine": transformation_engine,
            "project": self.project,
            "layer": layer,  # or dynamically if you want
            "environment": self.environment,
            "description": description,
            "status": status,
            "error_message": error_message,
            "start_timestamp": start_time.isoformat(),
            "end_timestamp": end_time.isoformat(),
            "duration_seconds": round((end_time - start_time).total_seconds(), 2)})

        return status == "SUCCESS"

    def _build_transformation_query(self) -> str:
        """
        Builds and returns an SQL query string to fetch active transformation configurations for a
        specific project and environment.

        This method constructs an SQL query that selects all columns from the `TransformationConfig`
        table in the `dbo` schema. The query filters records to include only those that are active
        and match the specified `project` and `environment` attributes of the instance.

        Returns:
            str: An SQL query string that can be executed to retrieve relevant transformation configuration data.

        Example:
            Assuming an instance `transformer` of a class with `project='DataAnalytics'` and
            `environment='production'` attributes:
            ```python
            query = transformer._build_transformation_query()
            print(query)
            ```
            This will output:
            ```
            SELECT *
            FROM [dbo].[TransformationConfig]
            WHERE project = 'DataAnalytics'
            AND environment = 'production'
            AND active = 1
            ```
        """
        return f"""
        SELECT *
        FROM [dbo].[TransformationConfig]
        WHERE project = '{self.project}'
          AND environment = '{self.environment}'
          AND active = 1
        """

    def _generate_markdown_from_doc_steps(self) -> str:
        """
        Generates a Markdown formatted string that documents transformation steps stored in `self.doc_steps`.

        This method organizes and formats the documentation of transformation steps, which include details
        such as the source and target data objects, transformation descriptions, and configuration parameters.
        The output is structured with Markdown syntax to enhance readability and organization, suitable for
        reports or version control documentation.

        Returns:
            str: A string formatted in Markdown that documents all the transformation steps. If `self.doc_steps`
            is empty, it returns a message indicating no steps to document.

        Example:
            Assuming `self.doc_steps` is populated with relevant transformation steps, calling this method
            will return a Markdown formatted string that details each step, including source and target objects,
            transformation type, and configuration parameters.

            ```python
            markdown_documentation = instance._generate_markdown_from_doc_steps()
            print(markdown_documentation)
            ```

        Note:
            This method is intended to be used internally within the class, hence the underscore prefix
            suggesting its private nature.
        """
        if not self.doc_steps:
            return "# No executed steps to document."

        doc = "# Transformation Documentation\n\n"
        doc += f"**Project:** `{self.project}`  \n"
        doc += f"**Environment:** `{self.environment}`\n\n"
        doc += "---\n\n"

        for step_info in sorted(self.doc_steps, key=lambda x: x["step"]):
            doc += f"## Step {step_info['step']}: {step_info['transformer_type'].capitalize()} Transformation\n\n"
            doc += f"**Source Table/View:** `{step_info['source_object']}`  \n"
            doc += f"**Target View Created:** `{step_info['target_object']}`  \n"
            doc += f"**Description:** {step_info['description']}\n\n"

            # Pretty print configuration parameters
            pretty_config = json.dumps(step_info['config'], indent=2)

            doc += "**Configuration Parameters:**\n\n"
            doc += "```json\n"
            doc += f"{pretty_config}\n"
            doc += "```\n\n"
            doc += "---\n\n"

        return doc

    def get_audit_log(self) -> pd.DataFrame:
        """
        Fetches the audit log data and returns it as a Pandas DataFrame.

        This method retrieves the audit log data stored within the instance and converts it into a Pandas DataFrame
        for easy manipulation and analysis.

        Returns:
            pd.DataFrame: A DataFrame containing the audit log data, where each row represents an entry
            in the audit log.

        Example:
            >>> audit_instance = AuditLogClass()
            >>> audit_df = audit_instance.get_audit_log()
            >>> print(audit_df.head())
        """
        return pd.DataFrame(self.audit_log)
