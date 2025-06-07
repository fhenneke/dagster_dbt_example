# Dagster + dbt Incremental Pipeline Demo

This project demonstrates a complete data pipeline using Dagster and dbt with incremental processing, cross-partition dependencies, and 2-dimensional partitioning.

## Overview

The pipeline simulates a real-world scenario where:
- Daily raw data is processed and aggregated into weekly summaries
- dbt handles incremental transformations using time-based variables
- Dagster orchestrates the entire workflow with automatic triggering
- 2-dimensional partitioning handles both time windows and static categories

## Project Structure

```
dagster_and_dbt/
├── __init__.py
├── definitions.py          # Main Dagster definitions
├── assets.py              # All asset definitions (Dagster + dbt)
├── partitions.py          # Partition definitions
├── resources.py           # Resource configurations
├── project.py             # dbt project setup
├── jobs.py                # Job definitions (legacy)
└── dbt/                   # dbt project
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── sources.yml
        ├── staging/       # Incremental staging models
        └── marts/         # Incremental mart models
```

## Data Flow

```
Dagster Assets (Raw Data)
    ↓
dbt Staging Models (Incremental)
    ↓
dbt Mart Models (Incremental)
    ↓
Dagster Post-Processing
```

### Pipeline Steps

1. **Pre-processing (Dagster)**: 
   - `daily_raw_data`: Creates daily raw data with timestamp values
   - `weekly_raw_data`: Creates weekly raw data with timestamp values

2. **Staging (dbt)**: 
   - `stg_daily_raw_data`: Incremental staging of daily data
   - `stg_weekly_raw_data`: Incremental staging of weekly data

3. **Marts (dbt)**:
   - `mart_daily_data`: Simple incremental copy of daily staging
   - `mart_weekly_data`: Aggregates daily data by week + joins weekly data

4. **Post-processing (Dagster)**:
   - `post_processing_daily`: Processes daily mart output
   - `post_processing_weekly`: Processes weekly mart output

## Key Design Choices

### 2-Dimensional Partitioning

The project uses `MultiPartitionsDefinition` with two dimensions:
- **Time dimension**: Daily/weekly time windows
- **Static dimension**: Categories "A" and "B"

This allows for granular control over data processing while maintaining flexibility for different business requirements.

### Cross-Partition Dependencies

The weekly post-processing asset depends on daily mart data using automation conditions rather than explicit `AssetDep` mappings. This avoids complexity while maintaining proper execution order.

### Incremental Processing with Variables

All dbt models use incremental materialization with time-range variables:
```sql
{% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
{% endif %}
```

This ensures efficient processing of only new/changed data.

## Advanced Concepts

### Custom Dagster-dbt Translator

The `CustomizedDagsterDbtTranslator` class handles two key functions:

1. **Asset Key Mapping**: Maps dbt source tables to Dagster asset names
   ```python
   if resource_type == "source":
       return dg.AssetKey(name)  # Maps directly to asset name
   ```

2. **Automation Conditions**: Applies consistent automation logic to all dbt assets
   ```python
   def get_automation_condition(self, dbt_resource_props):
       return condition  # Global automation condition
   ```

This allows dbt models to automatically depend on Dagster preprocessing assets.

### Automation Conditions

The project uses sophisticated automation conditions to prevent race conditions:

```python
condition = (
    dg.AutomationCondition.eager().without(
        dg.AutomationCondition.in_latest_time_window()
    )
    & dg.AutomationCondition.all_deps_blocking_checks_passed()
)
```

This ensures:
- Assets run eagerly when dependencies are ready
- But not in the latest time window (avoids incomplete data)
- Only when all dependency checks pass

### Split dbt Assets by Tags

Instead of listing individual models, the project uses dbt tags for cleaner organization:

```python
@dbt_assets(select="tag:daily", partitions_def=daily_partition)
@dbt_assets(select="tag:weekly", partitions_def=weekly_partition)
```

Models are tagged in their configuration:
```sql
{{ config(materialized='incremental', tags=['daily']) }}
```

This approach scales better as more models are added - just tag them appropriately.

### DuckDB Configuration

The project uses environment variables for database configuration:
- Dagster uses `DUCKDB_DATABASE` env var
- dbt uses the same variable via `{{ env_var("DUCKDB_DATABASE", "data/data.duckdb") }}`

This ensures both tools access the same database file.

### Retry Configuration

Global retry policy is configured via `dagster.yaml`:
```yaml
run_retries:
  enabled: true
  max_retries: 3
```

This automatically retries failed runs without per-asset configuration.

## Running the Pipeline

1. Set environment variable: `export DUCKDB_DATABASE=data/data.duckdb`
2. Start Dagster UI: `dagster dev`
3. Materialize assets through the UI or let automation conditions trigger them

## Common Patterns Demonstrated

- **Incremental processing** with time-based filtering
- **Cross-partition dependencies** between daily and weekly assets
- **Mixed orchestration** with both Dagster and dbt
- **Automatic dependency detection** via dbt sources and translator
- **Tag-based model organization** for scalability
- **Environment-based configuration** for flexibility
- **Automation conditions** for intelligent triggering

This setup provides a solid foundation for production data pipelines requiring incremental processing, complex dependencies, and mixed tool orchestration.