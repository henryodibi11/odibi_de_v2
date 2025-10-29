import pytest
from unittest.mock import MagicMock, patch
from odibi_de_v2.databricks.workflows.bronze_pipeline import run_bronze_pipeline
from odibi_de_v2.databricks.config.pipeline_settings import BronzePipelineSettings


class TestBronzePipelineValidation:
    """Unit tests for bronze_pipeline input validation."""
    
    def test_missing_project_raises_error(self):
        """Test that missing project raises ValueError."""
        with pytest.raises(ValueError, match="Parameter 'project' is required"):
            run_bronze_pipeline(
                project="",
                table="test_table",
                domain="test",
                source_id="src1",
                target_id="tgt1"
            )
    
    def test_missing_table_raises_error(self):
        """Test that missing table raises ValueError."""
        with pytest.raises(ValueError, match="Parameter 'table' is required"):
            run_bronze_pipeline(
                project="test_project",
                table="",
                domain="test",
                source_id="src1",
                target_id="tgt1"
            )
    
    def test_missing_source_id_raises_error(self):
        """Test that missing source_id raises ValueError."""
        with pytest.raises(ValueError, match="Parameter 'source_id' is required"):
            run_bronze_pipeline(
                project="test_project",
                table="test_table",
                domain="test",
                source_id="",
                target_id="tgt1"
            )
    
    def test_missing_target_id_raises_error(self):
        """Test that missing target_id raises ValueError."""
        with pytest.raises(ValueError, match="Parameter 'target_id' is required"):
            run_bronze_pipeline(
                project="test_project",
                table="test_table",
                domain="test",
                source_id="src1",
                target_id=""
            )
    
    def test_invalid_environment_raises_error(self):
        """Test that invalid environment raises ValueError."""
        with pytest.raises(ValueError, match="Invalid environment"):
            run_bronze_pipeline(
                project="test_project",
                table="test_table",
                domain="test",
                source_id="src1",
                target_id="tgt1",
                config_environment="invalid"
            )
    
    @patch('odibi_de_v2.databricks.get_secret')
    @patch('odibi_de_v2.databricks.load_ingestion_configs_from_sql')
    @patch('odibi_de_v2.databricks.init_spark_with_azure_secrets')
    def test_empty_source_config_raises_error(self, mock_init_spark, mock_load_configs, mock_get_secret):
        """Test that empty source config raises ValueError."""
        mock_spark = MagicMock()
        mock_connector = MagicMock()
        mock_init_spark.return_value = (mock_spark, mock_connector)
        mock_get_secret.return_value = "test_secret"
        
        mock_source_df = MagicMock()
        mock_source_df.count.return_value = 0
        mock_target_df = MagicMock()
        mock_target_df.count.return_value = 1
        mock_load_configs.return_value = (mock_source_df, mock_target_df)
        
        with pytest.raises(ValueError, match="No source config found"):
            run_bronze_pipeline(
                project="test_project",
                table="test_table",
                domain="test",
                source_id="src1",
                target_id="tgt1"
            )
    
    @patch('odibi_de_v2.databricks.get_secret')
    @patch('odibi_de_v2.databricks.load_ingestion_configs_from_sql')
    @patch('odibi_de_v2.databricks.init_spark_with_azure_secrets')
    def test_empty_target_config_raises_error(self, mock_init_spark, mock_load_configs, mock_get_secret):
        """Test that empty target config raises ValueError."""
        mock_spark = MagicMock()
        mock_connector = MagicMock()
        mock_init_spark.return_value = (mock_spark, mock_connector)
        mock_get_secret.return_value = "test_secret"
        
        mock_source_df = MagicMock()
        mock_source_df.count.return_value = 1
        mock_target_df = MagicMock()
        mock_target_df.count.return_value = 0
        mock_load_configs.return_value = (mock_source_df, mock_target_df)
        
        with pytest.raises(ValueError, match="No target config found"):
            run_bronze_pipeline(
                project="test_project",
                table="test_table",
                domain="test",
                source_id="src1",
                target_id="tgt1"
            )


class TestBronzePipelineSettings:
    """Unit tests for BronzePipelineSettings dataclass."""
    
    def test_default_settings(self):
        """Test default settings initialization."""
        settings = BronzePipelineSettings()
        assert settings.secret_scope == "GOATKeyVault"
        assert settings.blob_name_key == "GoatBlobStorageName"
        assert settings.environment == "qat"
    
    def test_custom_settings(self):
        """Test custom settings initialization."""
        settings = BronzePipelineSettings(
            secret_scope="MyVault",
            environment="prod"
        )
        assert settings.secret_scope == "MyVault"
        assert settings.environment == "prod"
    
    def test_environment_normalization(self):
        """Test environment is normalized to lowercase."""
        settings = BronzePipelineSettings(environment="PROD")
        assert settings.environment == "prod"
    
    def test_invalid_environment_raises_error(self):
        """Test invalid environment raises ValueError."""
        with pytest.raises(ValueError, match="Invalid environment"):
            BronzePipelineSettings(environment="invalid")
    
    @patch('odibi_de_v2.logger.get_logger')
    @patch('odibi_de_v2.databricks.SparkDataSaverFromConfig')
    @patch('odibi_de_v2.databricks.SparkDataReaderFromConfig')
    @patch('odibi_de_v2.databricks.IngestionConfigConstructor')
    @patch('odibi_de_v2.databricks.get_secret')
    @patch('odibi_de_v2.databricks.load_ingestion_configs_from_sql')
    @patch('odibi_de_v2.databricks.init_spark_with_azure_secrets')
    def test_pipeline_uses_settings(self, mock_init_spark, mock_load_configs, mock_get_secret,
                                   mock_constructor, mock_reader, mock_saver, mock_logger):
        """Test that pipeline uses BronzePipelineSettings correctly."""
        mock_logger_instance = MagicMock()
        mock_logger_instance.get_logs.return_value = []
        mock_logger.return_value = mock_logger_instance
        
        mock_spark = MagicMock()
        mock_connector = MagicMock()
        mock_init_spark.return_value = (mock_spark, mock_connector)
        mock_get_secret.return_value = "test_secret"
        
        mock_source_df = MagicMock()
        mock_source_df.count.return_value = 1
        mock_target_df = MagicMock()
        mock_target_df.count.return_value = 1
        mock_load_configs.return_value = (mock_source_df, mock_target_df)
        
        mock_config_obj = MagicMock()
        mock_config_obj.prepare.return_value = (MagicMock(), MagicMock())
        mock_constructor.return_value = mock_config_obj
        
        mock_reader_instance = MagicMock()
        mock_reader_instance.read_data.return_value = MagicMock()
        mock_reader.return_value = mock_reader_instance
        
        mock_saver_instance = MagicMock()
        mock_saver.return_value = mock_saver_instance
        
        settings = BronzePipelineSettings(
            secret_scope="CustomVault",
            environment="dev"
        )
        
        run_bronze_pipeline(
            project="test_project",
            table="test_table",
            domain="test",
            source_id="src1",
            target_id="tgt1",
            settings=settings
        )
        
        mock_init_spark.assert_called_once()
        assert mock_init_spark.call_args[1]['secret_scope'] == "CustomVault"
        
        mock_load_configs.assert_called_once()
        assert mock_load_configs.call_args[1]['environment'] == "dev"
