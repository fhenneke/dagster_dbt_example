# Dagster + dbt Incremental Pipeline Demo

This project demonstrates a complete data pipeline using Dagster and dbt with incremental processing, cross-partition dependencies, and 2-dimensional partitioning.

## Overview

The pipeline simulates a real-world scenario where:
- Only initial assets run on cron schedules (daily at 1 AM, weekly on Tuesdays at 1 AM)
- All downstream assets automatically materialize when dependencies are updated
- dbt handles incremental transformations using partition start/end times as variables
- Cross-partition dependencies allow weekly models to depend on daily data
- 2-dimensional partitioning handles both time windows and static categories

## Incremental Processing with Partition Variables

All dbt models use incremental materialization with partition-specific time ranges:
```sql
{% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
{% endif %}
```

Dagster passes the exact start and end time of each partition to dbt, ensuring only relevant data is processed incrementally.

## Project Structure

```
dagster_and_dbt/
├── __init__.py
├── definitions.py          # Main Dagster definitions
├── assets.py              # All asset definitions (Dagster + dbt)
├── partitions.py          # Partition definitions
├── resources.py           # Resource configurations
├── project.py             # dbt project setup
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

### Split dbt Assets into Daily and Weekly Models

dbt models are split into separate daily and weekly assets with cross-partition dependencies:

```python
@dbt_assets(select="tag:daily", partitions_def=daily_partition)
@dbt_assets(select="tag:weekly", partitions_def=weekly_partition)
```

The weekly mart model depends on daily staging data, demonstrating how dbt can handle cross-partition dependencies within its execution context while Dagster manages the overall orchestration.

### DuckDB Configuration

The project uses DuckDB as the simplest choice for adding a database component - this has no impact on the core concepts being demonstrated. Configuration uses environment variables:
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

## Getting Started

### Quick Setup and Testing

1. **Install dependencies**: `uv sync`
2. **Activate environment**: `source .venv/bin/activate`
3. **Setup environment**: `cp .env.example .env`
4. **Start Dagster UI**: `dagster dev`
5. **Open browser**: Navigate to `localhost:3000`
6. **Test the pipeline**: 
   - Go to Assets > Lineage view
   - Manually materialize the initial raw assets (`daily_raw_data`, `weekly_raw_data`)
   - Watch as the entire pipeline automatically executes downstream assets
   - Observe how dbt models and post-processing assets trigger automatically

### Production Usage

In production, only the initial assets need scheduling - they run on cron schedules:
- `daily_raw_data`: Daily at 1 AM
- `weekly_raw_data`: Tuesdays at 1 AM

All other assets automatically materialize when their dependencies are updated.

## Common Patterns Demonstrated

- **Incremental processing** with time-based filtering
- **Cross-partition dependencies** between daily and weekly assets
- **Mixed orchestration** with both Dagster and dbt
- **Automatic dependency detection** via dbt sources and translator
- **Automation conditions** for intelligent triggering

This setup provides a solid foundation for production data pipelines requiring incremental processing, complex dependencies, and mixed tool orchestration.
