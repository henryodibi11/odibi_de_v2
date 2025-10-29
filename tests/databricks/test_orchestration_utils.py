import pytest
from unittest.mock import MagicMock, patch, call, Mock
import time


@pytest.mark.skip(reason="Requires Databricks DBUtils which is not available in test environment")
class TestRunNotebookWithLogging:
    """Unit tests for run_notebook_with_logging."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        return MagicMock()
    
    @pytest.fixture
    def mock_dbutils(self):
        """Create a mock DBUtils."""
        dbutils = MagicMock()
        dbutils.notebook.run.return_value = None
        return dbutils
    
    @patch('time.time')
    @patch('odibi_de_v2.databricks.log_to_centralized_table')
    @patch('odibi_de_v2.databricks.utils.orchestration_utils.DBUtils')
    @patch('odibi_de_v2.logger.get_logger')
    def test_successful_execution(self, mock_get_logger, mock_dbutils_class, mock_log_central, mock_time, mock_spark):
        """Test successful notebook execution."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dbutils = MagicMock()
        mock_dbutils_class.return_value = mock_dbutils
        
        mock_time.side_effect = [100.0, 110.5]
        
        from odibi_de_v2.databricks.utils.orchestration_utils import run_notebook_with_logging
        run_notebook_with_logging(
            spark=mock_spark,
            notebook_path="/test/notebook",
            logic_app_url="https://test.com",
            base_path="/logs",
            table_name="execution_logs",
            timeout_seconds=3600
        )
        
        mock_dbutils.notebook.run.assert_called_once_with("/test/notebook", timeout_seconds=3600)
        mock_log_central.assert_called_once()
        args = mock_log_central.call_args[1]
        assert args['status'] == 'Success'
        assert args['notebook_path'] == '/test/notebook'
        assert 'execution_time_seconds' in args
    
    @patch('odibi_de_v2.utils.send_email_using_logic_app')
    @patch('odibi_de_v2.databricks.log_to_centralized_table')
    @patch('odibi_de_v2.databricks.utils.orchestration_utils.DBUtils')
    @patch('odibi_de_v2.logger.get_logger')
    def test_failure_with_email_alert(self, mock_get_logger, mock_dbutils_class, mock_log_central, mock_send_email, mock_spark):
        """Test notebook execution failure with email alert."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dbutils = MagicMock()
        mock_dbutils_class.return_value = mock_dbutils
        
        test_error = Exception("Notebook failed")
        mock_dbutils.notebook.run.side_effect = test_error
        
        from odibi_de_v2.databricks.utils.orchestration_utils import run_notebook_with_logging
        with pytest.raises(Exception, match="Notebook failed"):
            run_notebook_with_logging(
                spark=mock_spark,
                notebook_path="/test/notebook",
                logic_app_url="https://test.com",
                base_path="/logs",
                table_name="execution_logs",
                alert_email="test@example.com"
            )
        
        mock_log_central.assert_called_once()
        args = mock_log_central.call_args[1]
        assert args['status'] == 'Failure'
        assert 'Notebook failed' in args['error_message']
        
        mock_send_email.assert_called_once()
        email_args = mock_send_email.call_args[1]
        assert 'test@example.com' in email_args['to']
        assert 'Notebook Execution Failure' in email_args['subject']
    
    @patch('time.sleep')
    @patch('time.time')
    @patch('odibi_de_v2.databricks.log_to_centralized_table')
    @patch('odibi_de_v2.databricks.utils.orchestration_utils.DBUtils')
    @patch('odibi_de_v2.logger.get_logger')
    def test_retry_logic_success_on_second_attempt(self, mock_get_logger, mock_dbutils_class, mock_log_central, mock_time, mock_sleep, mock_spark):
        """Test retry logic succeeds on second attempt."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dbutils = MagicMock()
        mock_dbutils_class.return_value = mock_dbutils
        
        mock_dbutils.notebook.run.side_effect = [
            Exception("Transient error"),
            None
        ]
        
        mock_time.side_effect = [100.0, 100.0, 110.0]
        
        from odibi_de_v2.databricks.utils.orchestration_utils import run_notebook_with_logging
        run_notebook_with_logging(
            spark=mock_spark,
            notebook_path="/test/notebook",
            logic_app_url="https://test.com",
            base_path="/logs",
            table_name="execution_logs",
            retries=2,
            retry_delay_seconds=5
        )
        
        assert mock_dbutils.notebook.run.call_count == 2
        mock_sleep.assert_called_once_with(5)
        mock_log_central.assert_called_once()
        args = mock_log_central.call_args[1]
        assert args['status'] == 'Success'
    
    @patch('time.sleep')
    @patch('odibi_de_v2.utils.send_email_using_logic_app')
    @patch('odibi_de_v2.databricks.log_to_centralized_table')
    @patch('odibi_de_v2.databricks.utils.orchestration_utils.DBUtils')
    @patch('odibi_de_v2.logger.get_logger')
    def test_retry_logic_all_attempts_fail(self, mock_get_logger, mock_dbutils_class, mock_log_central, mock_send_email, mock_sleep, mock_spark):
        """Test retry logic when all attempts fail."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dbutils = MagicMock()
        mock_dbutils_class.return_value = mock_dbutils
        
        test_error = Exception("Persistent error")
        mock_dbutils.notebook.run.side_effect = test_error
        
        from odibi_de_v2.databricks.utils.orchestration_utils import run_notebook_with_logging
        with pytest.raises(Exception, match="Persistent error"):
            run_notebook_with_logging(
                spark=mock_spark,
                notebook_path="/test/notebook",
                logic_app_url="https://test.com",
                base_path="/logs",
                table_name="execution_logs",
                retries=2,
                retry_delay_seconds=1,
                alert_email="test@example.com"
            )
        
        assert mock_dbutils.notebook.run.call_count == 3
        assert mock_sleep.call_count == 2
        
        email_args = mock_send_email.call_args[1]
        assert "(after 3 attempts)" in email_args['subject']
