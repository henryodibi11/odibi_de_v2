from dataclasses import dataclass
from typing import Optional


@dataclass
class BronzePipelineSettings:
    """
    Configuration settings for Bronze layer pipeline execution.
    
    Consolidates secret key names and environment settings for cleaner function signatures.
    
    Args:
        secret_scope: Databricks Key Vault scope name
        blob_name_key: Key for Azure Blob storage account name
        blob_key_key: Key for Azure Blob storage account key
        sql_server_key: Key for SQL Server hostname
        sql_db_key: Key for SQL database name
        sql_user_key: Key for SQL username
        sql_pass_key: Key for SQL password
        environment: Environment for config selection (dev/qat/prod)
        
    Example:
        >>> settings = BronzePipelineSettings(
        ...     secret_scope="MyKeyVault",
        ...     environment="prod"
        ... )
        >>> # Other settings use defaults
    """
    secret_scope: str = "GOATKeyVault"
    blob_name_key: str = "GoatBlobStorageName"
    blob_key_key: str = "GoatBlobStorageKey"
    sql_server_key: str = "goatSQLDBServer"
    sql_db_key: str = "goatSQLDB-DM"
    sql_user_key: str = "goatSQLDBUser"
    sql_pass_key: str = "goatSQLDBPass"
    environment: str = "qat"
    
    def __post_init__(self):
        """Validate environment after initialization."""
        valid_environments = ["dev", "qat", "prod"]
        env_lower = self.environment.lower()
        if env_lower not in valid_environments:
            raise ValueError(f"Invalid environment '{self.environment}'. Must be one of {valid_environments}")
        self.environment = env_lower
