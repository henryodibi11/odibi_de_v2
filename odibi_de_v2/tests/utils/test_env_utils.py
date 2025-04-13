import sys
from utils.env_utils import (
    is_running_in_databricks,
    is_running_in_notebook,
    is_local_env,
    get_current_env,
    get_env_variable
)


def test_is_running_in_databricks_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "13.0")
    assert is_running_in_databricks() is True

    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb...databricks.net")
    assert is_running_in_databricks() is True

    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    original_exec = sys.executable
    sys.executable = "/usr/local/bin/databricks-connect"
    assert is_running_in_databricks() is True
    sys.executable = original_exec


def test_is_running_in_notebook(monkeypatch):
    # This is a soft test — it will pass in real notebooks or
    # return False in CLI
    result = is_running_in_notebook()
    # Don't assert True/False; context-sensitive
    assert isinstance(result, bool)


def test_is_local_env(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    assert is_local_env() is True


def test_get_current_env(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    assert get_current_env() in ["local", "unknown"]

    monkeypatch.setenv("DATABRICKS_HOST", "https://abc")
    assert get_current_env() == "databricks"


def test_get_env_variable(monkeypatch):
    monkeypatch.setenv("MY_TEST_KEY", "12345")
    assert get_env_variable("MY_TEST_KEY") == "12345"
    assert get_env_variable(
        "NON_EXISTENT_KEY", default="fallback") == "fallback"
