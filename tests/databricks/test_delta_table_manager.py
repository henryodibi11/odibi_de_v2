import pytest
from unittest.mock import MagicMock, patch
from odibi_de_v2.databricks.delta.delta_table_manager import DeltaTableManager


class TestDeltaTableManager:
    """Unit tests for DeltaTableManager."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = MagicMock()
        spark.catalog = MagicMock()
        return spark
    
    @pytest.fixture
    def mock_delta_table(self):
        """Create a mock DeltaTable."""
        delta_table = MagicMock()
        return delta_table
    
    def test_init_table_mode(self, mock_spark):
        """Test initialization in table mode."""
        manager = DeltaTableManager(mock_spark, "test_db.test_table", is_path=False)
        assert manager.table_or_path == "test_db.test_table"
        assert manager.is_path is False
        assert manager.spark == mock_spark
    
    def test_init_path_mode(self, mock_spark):
        """Test initialization in path mode."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        assert manager.table_or_path == "dbfs:/path/to/delta"
        assert manager.is_path is True
    
    def test_time_travel_version_and_timestamp_raises_error(self, mock_spark):
        """Test that providing both version and timestamp raises ValueError."""
        manager = DeltaTableManager(mock_spark, "test_table")
        with pytest.raises(ValueError, match="Provide either version or timestamp, not both"):
            manager.time_travel(version=5, timestamp="2025-01-01")
    
    def test_time_travel_with_version(self, mock_spark):
        """Test time travel with version."""
        manager = DeltaTableManager(mock_spark, "test_table")
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.table.return_value = MagicMock()
        
        result = manager.time_travel(version=5)
        mock_reader.option.assert_called_once_with("versionAsOf", 5)
        mock_reader.table.assert_called_once_with("test_table")
    
    def test_time_travel_with_timestamp(self, mock_spark):
        """Test time travel with timestamp."""
        manager = DeltaTableManager(mock_spark, "test_table")
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.table.return_value = MagicMock()
        
        result = manager.time_travel(timestamp="2025-01-01")
        mock_reader.option.assert_called_once_with("timestampAsOf", "2025-01-01")
    
    def test_get_latest_version_empty_history_raises_error(self, mock_spark):
        """Test that empty history raises ValueError."""
        manager = DeltaTableManager(mock_spark, "test_table")
        
        with patch.object(manager, 'show_history') as mock_history:
            mock_df = MagicMock()
            mock_df.collect.return_value = []
            mock_history.return_value = mock_df
            
            with pytest.raises(ValueError, match="No history found for table/path"):
                manager.get_latest_version()
    
    def test_get_latest_version_success(self, mock_spark):
        """Test getting latest version successfully."""
        manager = DeltaTableManager(mock_spark, "test_table")
        
        with patch.object(manager, 'show_history') as mock_history:
            mock_df = MagicMock()
            mock_df.collect.return_value = [{"version": 10}]
            mock_history.return_value = mock_df
            
            version = manager.get_latest_version()
            assert version == 10
    
    def test_restore_version_path_mode_raises_error(self, mock_spark):
        """Test that restore_version raises error for path-based tables."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        
        with pytest.raises(ValueError, match="restore_version\\(\\) only supports metastore tables"):
            manager.restore_version(5)
    
    def test_restore_version_table_mode(self, mock_spark):
        """Test restore version for table mode."""
        manager = DeltaTableManager(mock_spark, "test_table")
        manager.restore_version(5)
        mock_spark.sql.assert_called_once_with("RESTORE TABLE test_table TO VERSION AS OF 5")
    
    def test_optimize_table_mode(self, mock_spark):
        """Test OPTIMIZE for table mode."""
        manager = DeltaTableManager(mock_spark, "test_table")
        manager.optimize()
        mock_spark.sql.assert_called_once_with("OPTIMIZE test_table")
    
    def test_optimize_path_mode(self, mock_spark):
        """Test OPTIMIZE for path mode with proper quoting."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        manager.optimize()
        mock_spark.sql.assert_called_once_with("OPTIMIZE delta.`dbfs:/path/to/delta`")
    
    def test_optimize_with_zorder(self, mock_spark):
        """Test OPTIMIZE with ZORDER."""
        manager = DeltaTableManager(mock_spark, "test_table")
        manager.optimize(zorder_by=["col1", "col2"])
        mock_spark.sql.assert_called_once_with("OPTIMIZE test_table ZORDER BY (col1, col2)")
    
    def test_vacuum_table_mode(self, mock_spark):
        """Test VACUUM for table mode."""
        manager = DeltaTableManager(mock_spark, "test_table")
        manager.vacuum(retention_hours=72, dry_run=False)
        mock_spark.sql.assert_called_once_with("VACUUM test_table RETAIN 72 HOURS")
    
    def test_vacuum_path_mode(self, mock_spark):
        """Test VACUUM for path mode with proper quoting."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        manager.vacuum(retention_hours=168, dry_run=True)
        mock_spark.sql.assert_called_once_with("VACUUM delta.`dbfs:/path/to/delta` RETAIN 168 HOURS DRY RUN")
    
    def test_cache_path_mode_raises_error(self, mock_spark):
        """Test that cache raises error for path-based tables."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        
        with pytest.raises(ValueError, match="cache\\(\\) only supports metastore tables"):
            manager.cache()
    
    def test_uncache_path_mode_raises_error(self, mock_spark):
        """Test that uncache raises error for path-based tables."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        
        with pytest.raises(ValueError, match="uncache\\(\\) only supports metastore tables"):
            manager.uncache()
    
    def test_recache_path_mode_raises_error(self, mock_spark):
        """Test that recache raises error for path-based tables."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        
        with pytest.raises(ValueError, match="recache\\(\\) only supports metastore tables"):
            manager.recache()
    
    def test_register_table_path_mode(self, mock_spark):
        """Test registering a path-based table."""
        manager = DeltaTableManager(mock_spark, "dbfs:/path/to/delta", is_path=True)
        manager.register_table("my_table", database="my_db")
        
        expected_sql = """
            CREATE TABLE IF NOT EXISTS my_db.my_table
            USING DELTA
            LOCATION 'dbfs:/path/to/delta'
        """
        mock_spark.sql.assert_called_once()
        actual_call = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS my_db.my_table" in actual_call
        assert "dbfs:/path/to/delta" in actual_call
    
    def test_register_table_table_mode_raises_error(self, mock_spark):
        """Test that register_table raises error for non-path tables."""
        manager = DeltaTableManager(mock_spark, "test_table", is_path=False)
        
        with pytest.raises(ValueError, match="register_table\\(\\) only works for path-based Delta tables"):
            manager.register_table("new_table")
    
    def test_uncache_all(self, mock_spark):
        """Test session-wide uncache all."""
        DeltaTableManager.uncache_all(mock_spark)
        mock_spark.catalog.clearCache.assert_called_once()
