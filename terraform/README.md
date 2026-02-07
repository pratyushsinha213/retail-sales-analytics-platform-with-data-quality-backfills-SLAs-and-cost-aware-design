# Terraform – AWS data lake and Glue

Naming convention: **pratyush_<resource_name>_<region>_<type_of_resource>**

- **Resource names in AWS** use this pattern (e.g. `pratyush_glue_job_us_east_1_role`, `pratyush_pipeline_us_east_1_job`).
- **S3 bucket names** use hyphens (AWS rule): e.g. `pratyush-datalake-us-east-1-dev-<account_id>`.
- **Terraform variables** are prefixed with `pratyush_` where they refer to a specific resource (e.g. `pratyush_s3_bucket_name`, `pratyush_glue_job_script_bucket`).

Creates:

- **S3 bucket** – data lake (bronze, silver, gold, raw, temp, spark-logs)
- **IAM role** – `pratyush_glue_job_<region>_role` for Glue (S3 + Glue Catalog)
- **Glue Catalog database** – `pratyush_appleretail_<region>_database`
- **Glue Job** – `pratyush_pipeline_<region>_job` (runs pipeline script from S3)

## Usage

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Set `pratyush_s3_bucket_name` (globally unique; use hyphens) and `pratyush_glue_job_script_bucket`.
3. Run:

   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

4. Upload **glue_pipeline.py** and **app.zip** to S3 (see project **AWS_SETUP.md** when added).

Add `terraform.tfvars` to `.gitignore` if it contains sensitive or environment-specific values.
