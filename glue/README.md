# Glue pipeline entrypoint

**glue_pipeline.py** is the script the AWS Glue job runs. It sets config from job parameters and runs:

1. `ingestion.ingest_raw` (reads from `s3://bucket/prefix/raw/*.csv`, writes Bronze)
2. `transformations.bronze_to_silver` (Bronze → Silver)
3. `transformations.silver_to_gold` (Silver → Gold)
4. `data_quality.run_checks` (Silver + Gold checks)

## Upload for Glue

1. **Upload the script**
   - Put `glue_pipeline.py` at  
     `s3://<pratyush_glue_job_script_bucket>/<pratyush_glue_job_script_key>/glue_pipeline.py`  
   - Example: `s3://pratyush-datalake-us-east-1-dev-123456789012/scripts/glue_pipeline.py`

2. **Create and upload app.zip**
   - From **Apple Retail v2** (project root):
   ```bash
   zip -r app.zip config utils ingestion transformations data_quality
   aws s3 cp app.zip s3://<bucket>/<scripts_prefix>/app.zip
   ```
   - The zip must contain top-level dirs: `config/`, `utils/`, `ingestion/`, `transformations/`, `data_quality/` (no `glue/`).

3. **Raw data in S3**
   - Upload the CSVs from `raw-data-source/` to  
     `s3://<bucket>/<prefix>/raw/`  
   - Files: `category.csv`, `products.csv`, `sales.csv`, `stores.csv`, `warranty.csv`

4. **Run the job**
   - In AWS Console: Glue → Jobs → `pratyush_pipeline_<region>_job` → Run.
   - Or: `aws glue start-job-run --job-name pratyush_pipeline_us_east_1_job`

Terraform sets the job’s `--s3_bucket`, `--s3_prefix`, `--region` and `--extra-py-files`; ensure the script and app.zip paths match your Terraform `pratyush_glue_job_script_bucket` and `pratyush_glue_job_script_key`.
