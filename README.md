# Unity Catalog Metadata Workflow

A Databricks workflow for collecting and analyzing Unity Catalog metadata, including table statistics, row counts, and usage information.

## Features

- **Metadata Collection**: Retrieve comprehensive table metadata from Unity Catalog
- **Bundle Deployment**: Deploy to your Databricks workspace via CLI
- **Usage Statistics**: Track table access patterns and user activity
- **Analysis & Visualization**: Interactive notebooks for data exploration
- **Export Capabilities**: JSON and CSV output formats

## Project Structure

```
workspace-management/
├── databricks.yml              # Bundle configuration
├── src/
│   ├── unity_catalog_client.py # Unity Catalog API client
│   ├── metadata_collector.py   # Core metadata collection logic
│   └── config.py               # Configuration management
├── notebooks/
│   └── metadata_analysis.py    # Analysis and visualization notebook
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## Quick Start

### 1. Prerequisites

- Databricks CLI v0.218.0 or higher
- Python 3.8+
- Access to a Databricks workspace with Unity Catalog enabled

### 2. Installation

```bash
# Install Databricks CLI
pip install databricks-cli>=0.218.0

# Install project dependencies
pip install -r requirements.txt

# Configure Databricks authentication
databricks configure --token
```

### 3. Environment Setup

Set the following environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-access-token"
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
```

### 4. Bundle Deployment

Deploy to your workspace:

```bash
# Deploy with default catalog/schema
databricks bundle deploy

# Deploy with custom catalog/schema
databricks bundle deploy --var catalog_name="your_catalog" --var schema_name="your_schema"

# Or use the deployment script
./deploy.sh main default
```

### 5. Run Metadata Collection

Execute the metadata collection job:

```bash
# Run with default parameters
python src/metadata_collector.py --catalog main --schema default

# Run with custom parameters
python src/metadata_collector.py \
  --catalog your_catalog \
  --schema your_schema \
  --include-usage-stats \
  --usage-days 30 \
  --output results.json
```

### 6. Analysis

Use the analysis notebook in Databricks:

1. Import `notebooks/metadata_analysis.py` into your workspace
2. Set the widget parameters (catalog, schema)
3. Run all cells to generate visualizations and insights

## Configuration

### Bundle Configuration

The `databricks.yml` file defines:

- **Workspace**: Single workspace configuration
- **Job Configuration**: Spark clusters and task definitions
- **Variables**: Catalog and schema parameters
- **Resources**: Job definitions and library dependencies

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_HOST` | Workspace URL | Yes |
| `DATABRICKS_TOKEN` | Access token | Yes |
| `DATABRICKS_SERVER_HOSTNAME` | Server hostname for SQL | Yes |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path | Yes |

## Usage Examples

### Basic Metadata Collection

```python
from src.unity_catalog_client import UnityCatalogClient
from src.config import MetadataConfig

config = MetadataConfig(
    catalog="main",
    schema="default",
    include_usage_stats=True
)

with UnityCatalogClient() as client:
    tables = client.get_schema_metadata("main", "default")
    for table in tables:
        print(f"{table.table}: {table.row_count} rows, {table.size_bytes} bytes")
```

### Advanced Analysis

```python
from src.metadata_collector import MetadataCollector

collector = MetadataCollector(config)
results = collector.collect_metadata()

# Print summary
collector.print_summary()

# Save results
output_file = collector.save_results("my_analysis.json")
```

## Collected Metadata

The workflow collects the following information for each table:

### Basic Information
- Table name, catalog, schema
- Table type (TABLE, VIEW, etc.)
- Owner and creation timestamps
- Comments and properties

### Storage Information
- Data source format (DELTA, PARQUET, etc.)
- Storage location
- Table size in bytes
- Number of files

### Statistics
- Row count
- Last updated timestamp
- Data freshness indicators

### Usage Information (if available)
- Access count over specified period
- Number of unique users
- Last access timestamp
- Query patterns

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify `DATABRICKS_TOKEN` is valid
   - Check workspace permissions

2. **SQL Warehouse Connection**
   - Ensure `DATABRICKS_HTTP_PATH` is correct
   - Verify SQL warehouse is running

3. **Bundle Deployment Failures**
   - Check Databricks CLI version (>= 0.218.0)
   - Verify workspace configuration

4. **Metadata Collection Issues**
   - Confirm Unity Catalog access permissions
   - Check if system tables are enabled

### Debug Mode

Enable debug logging:

```bash
python src/metadata_collector.py \
  --catalog main \
  --schema default \
  --log-level DEBUG
```

## Extension Ideas

This workflow can be extended for additional functionality:

- **Spark Performance Metrics**: Add cluster and job performance data
- **Data Lineage**: Track table dependencies and data flow
- **Cost Analysis**: Include compute and storage cost metrics
- **Automated Reporting**: Schedule regular metadata collection
- **Data Quality**: Add data profiling and quality metrics

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License.