# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Metadata Analysis
# MAGIC 
# MAGIC This notebook analyzes Unity Catalog metadata collected from tables within a specified schema.
# MAGIC It provides visualizations and insights about table statistics, usage patterns, and data distribution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

# Configure plotting
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC 
# MAGIC Define the catalog and schema to analyze, or load from existing metadata file.

# COMMAND ----------

# Widget parameters for interactive use
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("metadata_file", "", "Metadata File Path (optional)")
dbutils.widgets.dropdown("analysis_type", "overview", ["overview", "detailed", "usage"], "Analysis Type")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
metadata_file = dbutils.widgets.get("metadata_file")
analysis_type = dbutils.widgets.get("analysis_type")

print(f"Analyzing: {catalog}.{schema}")
print(f"Analysis Type: {analysis_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

def load_metadata(file_path=None, catalog_name=None, schema_name=None):
    """Load metadata either from file or collect fresh data"""
    
    if file_path and Path(file_path).exists():
        print(f"Loading metadata from file: {file_path}")
        with open(file_path, 'r') as f:
            return json.load(f)
    
    elif catalog_name and schema_name:
        print(f"Collecting fresh metadata for {catalog_name}.{schema_name}")
        
        # Import and run the metadata collector
        import sys
        sys.path.append('/Workspace/Repos/workspace-management/src')
        
        from metadata_collector import MetadataCollector
        from config import MetadataConfig
        
        config = MetadataConfig(
            catalog=catalog_name,
            schema=schema_name,
            include_usage_stats=True
        )
        
        collector = MetadataCollector(config)
        return collector.collect_metadata()
    
    else:
        raise ValueError("Either provide metadata_file path or catalog/schema names")

# Load the metadata
metadata = load_metadata(
    file_path=metadata_file if metadata_file else None,
    catalog_name=catalog if not metadata_file else None,
    schema_name=schema if not metadata_file else None
)

print(f"Loaded metadata for {len(metadata.get('tables', []))} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing

# COMMAND ----------

# Convert to DataFrame for easier analysis
tables_df = pd.DataFrame(metadata.get('tables', []))
summary = metadata.get('summary', {})

if not tables_df.empty:
    # Clean and process data
    tables_df['size_mb'] = tables_df['size_bytes'].fillna(0) / (1024 * 1024)
    tables_df['size_gb'] = tables_df['size_bytes'].fillna(0) / (1024 * 1024 * 1024)
    tables_df['row_count'] = tables_df['row_count'].fillna(0)
    tables_df['has_data'] = tables_df['row_count'] > 0
    
    # Parse timestamps
    if 'created_at' in tables_df.columns:
        tables_df['created_at'] = pd.to_datetime(tables_df['created_at'], errors='coerce')
    if 'last_updated' in tables_df.columns:
        tables_df['last_updated'] = pd.to_datetime(tables_df['last_updated'], errors='coerce')
    
    print(f"Processed {len(tables_df)} tables")
    print(f"Tables with data: {tables_df['has_data'].sum()}")
else:
    print("No table data found!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview Analysis

# COMMAND ----------

if analysis_type in ["overview", "detailed"]:
    print("="*60)
    print("UNITY CATALOG METADATA OVERVIEW")
    print("="*60)
    
    print(f"Catalog: {metadata['catalog']}")
    print(f"Schema: {metadata['schema']}")
    print(f"Collection Time: {metadata['collection_timestamp']}")
    print(f"Total Tables: {len(tables_df)}")
    
    if not tables_df.empty:
        print(f"Tables with Data: {tables_df['has_data'].sum()}")
        print(f"Empty Tables: {(~tables_df['has_data']).sum()}")
        print(f"Total Size: {tables_df['size_gb'].sum():.2f} GB")
        print(f"Total Rows: {tables_df['row_count'].sum():,.0f}")
        
        if tables_df['row_count'].sum() > 0:
            print(f"Average Rows per Table: {tables_df['row_count'].mean():,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Distribution Visualizations

# COMMAND ----------

if not tables_df.empty and analysis_type in ["overview", "detailed"]:
    
    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Unity Catalog Metadata Analysis: {catalog}.{schema}', fontsize=16)
    
    # 1. Table size distribution
    non_zero_sizes = tables_df[tables_df['size_gb'] > 0]['size_gb']
    if len(non_zero_sizes) > 0:
        axes[0, 0].hist(non_zero_sizes, bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].set_title('Table Size Distribution (GB)')
        axes[0, 0].set_xlabel('Size (GB)')
        axes[0, 0].set_ylabel('Number of Tables')
        axes[0, 0].set_yscale('log')
    else:
        axes[0, 0].text(0.5, 0.5, 'No size data available', ha='center', va='center', transform=axes[0, 0].transAxes)
        axes[0, 0].set_title('Table Size Distribution (GB)')
    
    # 2. Row count distribution
    non_zero_rows = tables_df[tables_df['row_count'] > 0]['row_count']
    if len(non_zero_rows) > 0:
        axes[0, 1].hist(non_zero_rows, bins=20, alpha=0.7, color='lightgreen', edgecolor='black')
        axes[0, 1].set_title('Row Count Distribution')
        axes[0, 1].set_xlabel('Number of Rows')
        axes[0, 1].set_ylabel('Number of Tables')
        axes[0, 1].set_xscale('log')
        axes[0, 1].set_yscale('log')
    else:
        axes[0, 1].text(0.5, 0.5, 'No row count data available', ha='center', va='center', transform=axes[0, 1].transAxes)
        axes[0, 1].set_title('Row Count Distribution')
    
    # 3. Table types
    if 'table_type' in tables_df.columns:
        table_type_counts = tables_df['table_type'].value_counts()
        table_type_counts.plot(kind='bar', ax=axes[1, 0], color='coral')
        axes[1, 0].set_title('Table Types')
        axes[1, 0].set_xlabel('Table Type')
        axes[1, 0].set_ylabel('Count')
        axes[1, 0].tick_params(axis='x', rotation=45)
    else:
        axes[1, 0].text(0.5, 0.5, 'No table type data available', ha='center', va='center', transform=axes[1, 0].transAxes)
        axes[1, 0].set_title('Table Types')
    
    # 4. Data formats
    if 'data_source_format' in tables_df.columns:
        format_counts = tables_df['data_source_format'].value_counts()
        format_counts.plot(kind='bar', ax=axes[1, 1], color='gold')
        axes[1, 1].set_title('Data Source Formats')
        axes[1, 1].set_xlabel('Format')
        axes[1, 1].set_ylabel('Count')
        axes[1, 1].tick_params(axis='x', rotation=45)
    else:
        axes[1, 1].text(0.5, 0.5, 'No format data available', ha='center', va='center', transform=axes[1, 1].transAxes)
        axes[1, 1].set_title('Data Source Formats')
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Tables Analysis

# COMMAND ----------

if not tables_df.empty and analysis_type in ["overview", "detailed"]:
    
    print("TOP 10 LARGEST TABLES BY SIZE")
    print("-" * 50)
    largest_tables = tables_df.nlargest(10, 'size_gb')[['table', 'size_gb', 'row_count', 'table_type']]
    for _, row in largest_tables.iterrows():
        print(f"{row['table']:<30} {row['size_gb']:>8.2f} GB {row['row_count']:>12,.0f} rows ({row['table_type']})")
    
    print("\nTOP 10 TABLES BY ROW COUNT")
    print("-" * 50)
    most_rows = tables_df.nlargest(10, 'row_count')[['table', 'row_count', 'size_gb', 'table_type']]
    for _, row in most_rows.iterrows():
        print(f"{row['table']:<30} {row['row_count']:>12,.0f} rows {row['size_gb']:>8.2f} GB ({row['table_type']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Statistics Analysis

# COMMAND ----------

if analysis_type in ["usage", "detailed"] and not tables_df.empty:
    
    # Check if usage stats are available
    usage_columns = [col for col in tables_df.columns if 'usage_stats' in str(col)]
    
    if any(tables_df['usage_stats'].notna() if 'usage_stats' in tables_df.columns else []):
        print("USAGE STATISTICS ANALYSIS")
        print("-" * 50)
        
        # Extract usage statistics
        usage_data = []
        for _, row in tables_df.iterrows():
            if pd.notna(row.get('usage_stats')) and row['usage_stats']:
                usage_stats = row['usage_stats']
                usage_data.append({
                    'table': row['table'],
                    'access_count': usage_stats.get('access_count', 0),
                    'unique_users': usage_stats.get('unique_users', 0),
                    'last_accessed': usage_stats.get('last_accessed')
                })
        
        if usage_data:
            usage_df = pd.DataFrame(usage_data)
            
            # Most accessed tables
            print("TOP 10 MOST ACCESSED TABLES")
            print("-" * 40)
            top_accessed = usage_df.nlargest(10, 'access_count')
            for _, row in top_accessed.iterrows():
                print(f"{row['table']:<30} {row['access_count']:>6} accesses, {row['unique_users']:>3} users")
            
            # Create usage visualization
            if len(usage_df) > 0:
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
                
                # Access count distribution
                ax1.hist(usage_df['access_count'], bins=20, alpha=0.7, color='lightblue', edgecolor='black')
                ax1.set_title('Table Access Count Distribution')
                ax1.set_xlabel('Access Count')
                ax1.set_ylabel('Number of Tables')
                
                # Unique users distribution
                ax2.hist(usage_df['unique_users'], bins=20, alpha=0.7, color='lightcoral', edgecolor='black')
                ax2.set_title('Unique Users per Table Distribution')
                ax2.set_xlabel('Unique Users')
                ax2.set_ylabel('Number of Tables')
                
                plt.tight_layout()
                plt.show()
        else:
            print("No detailed usage statistics available")
    else:
        print("Usage statistics not available in the metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Table Information

# COMMAND ----------

if analysis_type == "detailed" and not tables_df.empty:
    
    print("DETAILED TABLE INFORMATION")
    print("=" * 80)
    
    # Sort by size for detailed view
    detailed_df = tables_df.sort_values('size_gb', ascending=False)
    
    for _, row in detailed_df.head(20).iterrows():  # Show top 20 tables
        print(f"\nTable: {row['table']}")
        print(f"  Type: {row.get('table_type', 'Unknown')}")
        print(f"  Owner: {row.get('owner', 'Unknown')}")
        print(f"  Size: {row['size_gb']:.2f} GB ({row['size_mb']:.2f} MB)")
        print(f"  Rows: {row['row_count']:,.0f}")
        print(f"  Format: {row.get('data_source_format', 'Unknown')}")
        
        if pd.notna(row.get('created_at')):
            print(f"  Created: {row['created_at']}")
        if pd.notna(row.get('last_updated')):
            print(f"  Last Updated: {row['last_updated']}")
        if row.get('location'):
            print(f"  Location: {row['location']}")
        if row.get('comment'):
            print(f"  Comment: {row['comment']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results

# COMMAND ----------

# Export processed data
output_path = f"/tmp/metadata_analysis_{catalog}_{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Save summary statistics
summary_stats = {
    'catalog': catalog,
    'schema': schema,
    'analysis_timestamp': datetime.now().isoformat(),
    'total_tables': len(tables_df),
    'tables_with_data': tables_df['has_data'].sum() if not tables_df.empty else 0,
    'total_size_gb': tables_df['size_gb'].sum() if not tables_df.empty else 0,
    'total_rows': tables_df['row_count'].sum() if not tables_df.empty else 0,
    'largest_table': tables_df.loc[tables_df['size_gb'].idxmax(), 'table'] if not tables_df.empty and tables_df['size_gb'].max() > 0 else None,
    'most_rows_table': tables_df.loc[tables_df['row_count'].idxmax(), 'table'] if not tables_df.empty and tables_df['row_count'].max() > 0 else None
}

# Save to JSON
with open(f"{output_path}_summary.json", 'w') as f:
    json.dump(summary_stats, f, indent=2, default=str)

# Save detailed table data to CSV
if not tables_df.empty:
    tables_df.to_csv(f"{output_path}_tables.csv", index=False)

print(f"Analysis results exported to:")
print(f"  Summary: {output_path}_summary.json")
print(f"  Tables: {output_path}_tables.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Recommendations

# COMMAND ----------

print("ANALYSIS COMPLETE")
print("=" * 50)

if not tables_df.empty:
    # Calculate some key metrics
    avg_size = tables_df['size_gb'].mean()
    median_size = tables_df['size_gb'].median()
    empty_tables_pct = ((~tables_df['has_data']).sum() / len(tables_df)) * 100
    
    print(f"📊 Key Metrics:")
    print(f"  Average table size: {avg_size:.2f} GB")
    print(f"  Median table size: {median_size:.2f} GB")
    print(f"  Empty tables: {empty_tables_pct:.1f}%")
    
    print(f"\n🔍 Recommendations:")
    
    if empty_tables_pct > 20:
        print(f"  • Consider reviewing empty tables ({empty_tables_pct:.1f}% of total)")
    
    if tables_df['size_gb'].max() > 100:
        print(f"  • Large tables detected (>{tables_df['size_gb'].max():.1f} GB) - consider partitioning")
    
    if len(tables_df[tables_df['table_type'] == 'VIEW']) > 0:
        print(f"  • {len(tables_df[tables_df['table_type'] == 'VIEW'])} views found - verify they're still needed")
    
    if 'last_updated' in tables_df.columns:
        old_tables = tables_df[tables_df['last_updated'] < (datetime.now() - timedelta(days=90))]
        if len(old_tables) > 0:
            print(f"  • {len(old_tables)} tables haven't been updated in 90+ days")

print(f"\n✅ Analysis completed successfully for {catalog}.{schema}")

# COMMAND ----------