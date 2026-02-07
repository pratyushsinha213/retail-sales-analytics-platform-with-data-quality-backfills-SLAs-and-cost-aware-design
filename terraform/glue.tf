# Glue Catalog database: pratyush_<resource_name>_<region>_<type>
resource "aws_glue_catalog_database" "pratyush_appleretail_region_database" {
  name        = "pratyush_appleretail_${local.pratyush_region_normalized}_database"
  description = "Apple Retail data lake catalog"

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# Glue job: pratyush_<resource_name>_<region>_<type>
resource "aws_glue_job" "pratyush_pipeline_region_job" {
  name     = "pratyush_pipeline_${local.pratyush_region_normalized}_job"
  role_arn = aws_iam_role.pratyush_glue_job_region_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.pratyush_glue_job_script_bucket}/${var.pratyush_glue_job_script_key}/glue_pipeline.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                           = "python"
    "--job-bookmark-option"                    = "job-bookmark-disable"
    "--TempDir"                                = "s3://${aws_s3_bucket.pratyush_datalake_region_bucket.id}/temp/"
    "--enable-metrics"                         = "true"
    "--enable-continuous-cloudwatch-log-group" = "true"
    "--enable-spark-ui"                        = "true"
    "--spark-event-logs-path"                  = "s3://${aws_s3_bucket.pratyush_datalake_region_bucket.id}/spark-logs/"
    "--extra-py-files"                         = "s3://${var.pratyush_glue_job_script_bucket}/${var.pratyush_glue_job_script_key}/app.zip"
    "--s3_bucket"                              = aws_s3_bucket.pratyush_datalake_region_bucket.id
    "--s3_prefix"                              = var.environment
    "--region"                                 = var.aws_region
  }

  glue_version      = "4.0"
  worker_type       = var.pratyush_glue_worker_type
  number_of_workers = var.pratyush_glue_number_of_workers

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
