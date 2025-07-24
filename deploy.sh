#!/bin/bash

# Databricks Bundle Deployment Script
# Usage: ./deploy.sh [catalog] [schema]

set -e

# Default values
CATALOG=${1:-"main"}
SCHEMA=${2:-"default"}

echo "🚀 Deploying Unity Catalog Metadata Workflow"
echo "Catalog: $CATALOG"
echo "Schema: $SCHEMA"
echo ""

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not found. Please install it first:"
    echo "   pip install databricks-cli>=0.218.0"
    exit 1
fi

# Check CLI version
CLI_VERSION=$(databricks --version | cut -d' ' -f3)
echo "✅ Databricks CLI version: $CLI_VERSION"

# Check if authenticated
if ! databricks workspace list &> /dev/null; then
    echo "❌ Databricks CLI not authenticated. Please run:"
    echo "   databricks configure --token"
    exit 1
fi

echo "✅ Databricks CLI authenticated"

# Validate bundle configuration
echo ""
echo "🔍 Validating bundle configuration..."
if ! databricks bundle validate; then
    echo "❌ Bundle validation failed"
    exit 1
fi

echo "✅ Bundle validation passed"

# Deploy the bundle
echo ""
echo "📦 Deploying bundle..."
if ! databricks bundle deploy --var catalog_name="$CATALOG" --var schema_name="$SCHEMA"; then
    echo "❌ Bundle deployment failed"
    exit 1
fi

echo "✅ Bundle deployed successfully"

# Run the metadata collection job
echo ""
echo "🔄 Running metadata collection job..."
JOB_ID=$(databricks jobs list --output json | jq -r ".jobs[] | select(.settings.name | contains(\"Unity Catalog Metadata Collection\")) | .job_id")

if [ -z "$JOB_ID" ]; then
    echo "❌ Could not find deployed job"
    exit 1
fi

echo "Job ID: $JOB_ID"

# Start the job run
RUN_ID=$(databricks jobs run --job-id "$JOB_ID" --json | jq -r '.run_id')
echo "Run ID: $RUN_ID"

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "📊 Monitor the job run:"
echo "   databricks runs get --run-id $RUN_ID"
echo ""
echo "🌐 View in workspace:"
echo "   https://$(echo $DATABRICKS_HOST | sed 's|https://||')/#job/$JOB_ID/run/$RUN_ID"
echo ""
echo "📝 To collect metadata manually:"
echo "   python src/metadata_collector.py --catalog $CATALOG --schema $SCHEMA"