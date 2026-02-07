output "pratyush_s3_bucket_id" {
  description = "Data lake S3 bucket name (pratyush_datalake_region_bucket)"
  value       = aws_s3_bucket.pratyush_datalake_region_bucket.id
}

output "pratyush_s3_bucket_arn" {
  description = "Data lake S3 bucket ARN"
  value       = aws_s3_bucket.pratyush_datalake_region_bucket.arn
}

output "pratyush_glue_job_name" {
  description = "Glue pipeline job name (pratyush_pipeline_region_job)"
  value       = aws_glue_job.pratyush_pipeline_region_job.name
}

output "pratyush_glue_job_role_arn" {
  description = "IAM role ARN used by the Glue job (pratyush_glue_job_region_role)"
  value       = aws_iam_role.pratyush_glue_job_region_role.arn
}

output "pratyush_glue_catalog_database" {
  description = "Glue Catalog database name (pratyush_appleretail_region_database)"
  value       = aws_glue_catalog_database.pratyush_appleretail_region_database.name
}
