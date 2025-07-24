import os
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from databricks.sql import connect
from databricks import sql
import requests


@dataclass
class TableMetadata:
    catalog: str
    schema: str
    table: str
    table_type: str
    owner: str
    created_at: Optional[str] = None
    last_updated: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    location: Optional[str] = None
    data_source_format: Optional[str] = None
    comment: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class UnityCtDataMissingError(Exception):
    pass


class UnityCatalogClient:
    def __init__(self, 
                 server_hostname: Optional[str] = None,
                 http_path: Optional[str] = None,
                 access_token: Optional[str] = None):
        self.server_hostname = server_hostname or os.getenv('DATABRICKS_SERVER_HOSTNAME')
        self.http_path = http_path or os.getenv('DATABRICKS_HTTP_PATH')
        self.access_token = access_token or os.getenv('DATABRICKS_TOKEN')
        
        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError("Missing required Databricks connection parameters")
        
        self.logger = logging.getLogger(__name__)
        self._connection = None
    
    def __enter__(self):
        self._connection = connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            self._connection.close()
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        if not self._connection:
            raise RuntimeError("Client not initialized. Use within context manager.")
        
        cursor = self._connection.cursor()
        try:
            self.logger.debug(f"Executing query: {query}")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results
        finally:
            cursor.close()
    
    def list_catalogs(self) -> List[str]:
        query = "SHOW CATALOGS"
        results = self._execute_query(query)
        return [row['catalog'] for row in results]
    
    def list_schemas(self, catalog: str) -> List[str]:
        query = f"SHOW SCHEMAS IN {catalog}"
        results = self._execute_query(query)
        return [row['databaseName'] for row in results]
    
    def list_tables(self, catalog: str, schema: str) -> List[str]:
        query = f"SHOW TABLES IN {catalog}.{schema}"
        results = self._execute_query(query)
        return [row['tableName'] for row in results]
    
    def get_table_info(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        full_table_name = f"{catalog}.{schema}.{table}"
        query = f"DESCRIBE TABLE EXTENDED {full_table_name}"
        results = self._execute_query(query)
        
        info = {}
        for row in results:
            col_name = row.get('col_name', '').strip()
            data_type = row.get('data_type', '').strip()
            comment = row.get('comment', '').strip()
            
            if col_name and not col_name.startswith('#'):
                if col_name in ['Type', 'Provider', 'Location', 'Owner', 'Created Time', 'Last Access']:
                    info[col_name.lower().replace(' ', '_')] = data_type
                elif col_name == 'Statistics':
                    if 'bytes' in data_type:
                        try:
                            info['size_bytes'] = int(data_type.split()[0])
                        except (ValueError, IndexError):
                            pass
                    if 'rows' in data_type:
                        try:
                            parts = data_type.split()
                            for i, part in enumerate(parts):
                                if part == 'rows' and i > 0:
                                    info['row_count'] = int(parts[i-1])
                                    break
                        except (ValueError, IndexError):
                            pass
        
        return info
    
    def get_table_detail(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        full_table_name = f"{catalog}.{schema}.{table}"
        query = f"DESCRIBE DETAIL {full_table_name}"
        try:
            results = self._execute_query(query)
            return results[0] if results else {}
        except Exception as e:
            self.logger.warning(f"Could not get table detail for {full_table_name}: {e}")
            return {}
    
    def get_table_metadata(self, catalog: str, schema: str, table: str) -> TableMetadata:
        full_table_name = f"{catalog}.{schema}.{table}"
        
        # Get basic table info
        table_info = self.get_table_info(catalog, schema, table)
        
        # Get detailed table information
        table_detail = self.get_table_detail(catalog, schema, table)
        
        # Try to get row count from table statistics
        row_count = None
        size_bytes = None
        
        if table_detail:
            # DESCRIBE DETAIL provides more accurate information
            row_count = table_detail.get('numFiles') or table_detail.get('numRows')
            size_bytes = table_detail.get('sizeInBytes')
        
        # Fallback to DESCRIBE TABLE EXTENDED info
        if row_count is None:
            row_count = table_info.get('row_count')
        if size_bytes is None:
            size_bytes = table_info.get('size_bytes')
        
        return TableMetadata(
            catalog=catalog,
            schema=schema,
            table=table,
            table_type=table_info.get('type', 'UNKNOWN'),
            owner=table_info.get('owner', 'UNKNOWN'),
            created_at=table_detail.get('createdAt') or table_info.get('created_time'),
            last_updated=table_detail.get('lastModified') or table_info.get('last_access'),
            row_count=row_count,
            size_bytes=size_bytes,
            location=table_detail.get('location') or table_info.get('location'),
            data_source_format=table_detail.get('format') or table_info.get('provider'),
            comment=table_detail.get('comment'),
            properties=table_detail.get('properties') if table_detail else None
        )
    
    def get_schema_metadata(self, catalog: str, schema: str) -> List[TableMetadata]:
        tables = self.list_tables(catalog, schema)
        metadata = []
        
        for table in tables:
            try:
                table_metadata = self.get_table_metadata(catalog, schema, table)
                metadata.append(table_metadata)
                self.logger.info(f"Collected metadata for {catalog}.{schema}.{table}")
            except Exception as e:
                self.logger.error(f"Failed to collect metadata for {catalog}.{schema}.{table}: {e}")
                continue
        
        return metadata
    
    def get_table_usage_stats(self, catalog: str, schema: str, table: str, 
                            days: int = 30) -> Dict[str, Any]:
        """Get table usage statistics from system tables if available"""
        full_table_name = f"{catalog}.{schema}.{table}"
        
        # Query system.access.table_lineage for usage information
        query = f"""
        SELECT 
            COUNT(*) as access_count,
            MAX(event_time) as last_accessed,
            COUNT(DISTINCT user_identity.email) as unique_users
        FROM system.access.table_lineage 
        WHERE target_table_full_name = '{full_table_name}'
        AND event_time >= CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
        """
        
        try:
            results = self._execute_query(query)
            return results[0] if results else {}
        except Exception as e:
            self.logger.warning(f"Could not get usage stats for {full_table_name}: {e}")
            return {}