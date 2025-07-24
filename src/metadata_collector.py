import argparse
import json
import logging
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import asdict
from pathlib import Path

from unity_catalog_client import UnityCatalogClient, TableMetadata
from config import MetadataConfig


class MetadataCollector:
    """Collects and processes Unity Catalog metadata for tables within a schema"""
    
    def __init__(self, config: MetadataConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.results = {
            'collection_timestamp': datetime.now().isoformat(),
            'catalog': config.catalog,
            'schema': config.schema,
            'tables': [],
            'summary': {}
        }
    
    def _setup_logging(self) -> logging.Logger:
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def collect_metadata(self) -> Dict[str, Any]:
        """Main method to collect metadata for all tables in the specified schema"""
        self.logger.info(f"Starting metadata collection for {self.config.catalog}.{self.config.schema}")
        
        try:
            with UnityCatalogClient() as client:
                # Verify catalog and schema exist
                catalogs = client.list_catalogs()
                if self.config.catalog not in catalogs:
                    raise ValueError(f"Catalog '{self.config.catalog}' not found. Available: {catalogs}")
                
                schemas = client.list_schemas(self.config.catalog)
                if self.config.schema not in schemas:
                    raise ValueError(f"Schema '{self.config.schema}' not found in {self.config.catalog}. Available: {schemas}")
                
                # Collect table metadata
                table_metadata = client.get_schema_metadata(self.config.catalog, self.config.schema)
                
                # Process and enrich metadata
                processed_tables = []
                for table_meta in table_metadata:
                    processed_table = self._process_table_metadata(client, table_meta)
                    processed_tables.append(processed_table)
                
                self.results['tables'] = processed_tables
                self.results['summary'] = self._generate_summary(processed_tables)
                
                self.logger.info(f"Collection completed. Found {len(processed_tables)} tables")
                return self.results
                
        except Exception as e:
            self.logger.error(f"Metadata collection failed: {e}")
            self.results['error'] = str(e)
            raise
    
    def _process_table_metadata(self, client: UnityCatalogClient, 
                              table_meta: TableMetadata) -> Dict[str, Any]:
        """Process and enrich individual table metadata"""
        table_dict = asdict(table_meta)
        
        # Add usage statistics if available
        if self.config.include_usage_stats:
            try:
                usage_stats = client.get_table_usage_stats(
                    table_meta.catalog, 
                    table_meta.schema, 
                    table_meta.table,
                    days=self.config.usage_days
                )
                table_dict['usage_stats'] = usage_stats
            except Exception as e:
                self.logger.warning(f"Could not get usage stats for {table_meta.table}: {e}")
                table_dict['usage_stats'] = {}
        
        # Add computed fields
        table_dict['size_mb'] = self._bytes_to_mb(table_meta.size_bytes)
        table_dict['size_gb'] = self._bytes_to_gb(table_meta.size_bytes)
        table_dict['has_data'] = table_meta.row_count is not None and table_meta.row_count > 0
        
        return table_dict
    
    def _bytes_to_mb(self, size_bytes: Optional[int]) -> Optional[float]:
        """Convert bytes to megabytes"""
        if size_bytes is None:
            return None
        return round(size_bytes / (1024 * 1024), 2)
    
    def _bytes_to_gb(self, size_bytes: Optional[int]) -> Optional[float]:
        """Convert bytes to gigabytes"""
        if size_bytes is None:
            return None
        return round(size_bytes / (1024 * 1024 * 1024), 2)
    
    def _generate_summary(self, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics for the collected metadata"""
        if not tables:
            return {'total_tables': 0}
        
        total_tables = len(tables)
        tables_with_data = sum(1 for t in tables if t.get('has_data', False))
        
        # Size statistics
        sizes = [t.get('size_bytes') for t in tables if t.get('size_bytes') is not None]
        total_size_bytes = sum(sizes) if sizes else 0
        
        # Row count statistics
        row_counts = [t.get('row_count') for t in tables if t.get('row_count') is not None]
        total_rows = sum(row_counts) if row_counts else 0
        
        # Table types
        table_types = {}
        for table in tables:
            table_type = table.get('table_type', 'UNKNOWN')
            table_types[table_type] = table_types.get(table_type, 0) + 1
        
        # Data formats
        data_formats = {}
        for table in tables:
            format_type = table.get('data_source_format', 'UNKNOWN')
            data_formats[format_type] = data_formats.get(format_type, 0) + 1
        
        return {
            'total_tables': total_tables,
            'tables_with_data': tables_with_data,
            'empty_tables': total_tables - tables_with_data,
            'total_size_bytes': total_size_bytes,
            'total_size_gb': self._bytes_to_gb(total_size_bytes),
            'total_rows': total_rows,
            'average_rows_per_table': round(total_rows / total_tables, 2) if total_tables > 0 else 0,
            'table_types': table_types,
            'data_formats': data_formats,
            'largest_table': max(tables, key=lambda t: t.get('size_bytes', 0), default={}).get('table'),
            'most_rows': max(tables, key=lambda t: t.get('row_count', 0), default={}).get('table')
        }
    
    def save_results(self, output_path: Optional[str] = None) -> str:
        """Save collection results to a JSON file"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"metadata_collection_{self.config.catalog}_{self.config.schema}_{timestamp}.json"
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to {output_file}")
        return str(output_file)
    
    def print_summary(self):
        """Print a formatted summary of the collection results"""
        summary = self.results.get('summary', {})
        
        print(f"\n{'='*60}")
        print(f"Unity Catalog Metadata Collection Summary")
        print(f"{'='*60}")
        print(f"Catalog: {self.results['catalog']}")
        print(f"Schema: {self.results['schema']}")
        print(f"Collection Time: {self.results['collection_timestamp']}")
        print(f"")
        
        if 'error' in self.results:
            print(f"❌ Collection failed: {self.results['error']}")
            return
        
        print(f"📊 Table Statistics:")
        print(f"  Total Tables: {summary.get('total_tables', 0)}")
        print(f"  Tables with Data: {summary.get('tables_with_data', 0)}")
        print(f"  Empty Tables: {summary.get('empty_tables', 0)}")
        print(f"")
        
        if summary.get('total_size_gb'):
            print(f"💾 Storage Statistics:")
            print(f"  Total Size: {summary.get('total_size_gb', 0):.2f} GB")
            print(f"  Total Rows: {summary.get('total_rows', 0):,}")
            print(f"  Average Rows/Table: {summary.get('average_rows_per_table', 0):,.2f}")
            print(f"")
        
        if summary.get('table_types'):
            print(f"📋 Table Types:")
            for table_type, count in summary['table_types'].items():
                print(f"  {table_type}: {count}")
            print(f"")
        
        if summary.get('data_formats'):
            print(f"📁 Data Formats:")
            for format_type, count in summary['data_formats'].items():
                print(f"  {format_type}: {count}")
            print(f"")
        
        if summary.get('largest_table'):
            print(f"🏆 Notable Tables:")
            print(f"  Largest Table: {summary.get('largest_table')}")
            print(f"  Most Rows: {summary.get('most_rows')}")


def main():
    """Command line entry point for metadata collection"""
    parser = argparse.ArgumentParser(description='Collect Unity Catalog metadata')
    parser.add_argument('--catalog', required=True, help='Catalog name')
    parser.add_argument('--schema', required=True, help='Schema name')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--include-usage-stats', action='store_true',
                       help='Include table usage statistics')
    parser.add_argument('--usage-days', type=int, default=30,
                       help='Number of days for usage statistics')
    
    args = parser.parse_args()
    
    config = MetadataConfig(
        catalog=args.catalog,
        schema=args.schema,
        log_level=args.log_level,
        include_usage_stats=args.include_usage_stats,
        usage_days=args.usage_days
    )
    
    collector = MetadataCollector(config)
    
    try:
        collector.collect_metadata()
        collector.print_summary()
        
        if args.output:
            collector.save_results(args.output)
        else:
            output_file = collector.save_results()
            print(f"\n💾 Results saved to: {output_file}")
    
    except Exception as e:
        logging.error(f"Collection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()