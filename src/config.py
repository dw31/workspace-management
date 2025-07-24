import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class MetadataConfig:
    """Configuration for Unity Catalog metadata collection"""
    catalog: str
    schema: str
    log_level: str = "INFO"
    include_usage_stats: bool = True
    usage_days: int = 30
    
    # Databricks connection settings
    server_hostname: Optional[str] = None
    http_path: Optional[str] = None
    access_token: Optional[str] = None
    
    def __post_init__(self):
        # Load from environment if not provided
        if self.server_hostname is None:
            self.server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        if self.http_path is None:
            self.http_path = os.getenv('DATABRICKS_HTTP_PATH')
        if self.access_token is None:
            self.access_token = os.getenv('DATABRICKS_TOKEN')


@dataclass
class WorkspaceConfig:
    """Configuration for Databricks workspace deployment"""
    host: str
    token: str
    workspace_id: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'WorkspaceConfig':
        """Create configuration from environment variables"""
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        workspace_id = os.getenv('DATABRICKS_WORKSPACE_ID')
        
        if not host or not token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required")
        
        return cls(host=host, token=token, workspace_id=workspace_id)


@dataclass
class BundleConfig:
    """Configuration for Databricks bundle deployment"""
    target: str = "dev"
    catalog: str = "main"
    schema: str = "default"
    
    def get_deployment_vars(self) -> dict:
        """Get variables for bundle deployment"""
        return {
            "catalog_name": self.catalog,
            "schema_name": self.schema
        }