## Development Workflow

All data transformation logic is developed locally using Python and PySpark and version-controlled via GitHub.

Azure services are used strictly for execution and orchestration:
- Azure Data Factory triggers Databricks jobs
- Databricks provides scalable Spark compute
- ADLS Gen2 stores versioned Delta Lake datasets

This setup enables reproducible deployments, safe backfills, and production-grade data engineering workflows.