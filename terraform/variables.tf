variable "project_name" {
  description = "Project name used in resource names"
  type        = string
  default     = "apple-retail"
}

variable "environment" {
  description = "Environment (e.g. dev, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region (e.g. us-east-1)"
  type        = string
  default     = "us-east-1"
}

variable "pratyush_s3_bucket_name" {
  description = "S3 bucket name for the data lake (must be globally unique). Convention: pratyush-datalake-<region>-<env>-<suffix>"
  type        = string
}

variable "pratyush_glue_job_script_bucket" {
  description = "S3 bucket where Glue job scripts are stored (can be same as pratyush_s3_bucket_name)"
  type        = string
}

variable "pratyush_glue_job_script_key" {
  description = "S3 key prefix for Glue job Python scripts"
  type        = string
  default     = "scripts"
}

variable "pratyush_glue_worker_type" {
  description = "Glue worker type (G.1X, G.2X, etc.)"
  type        = string
  default     = "G.1X"
}

variable "pratyush_glue_number_of_workers" {
  description = "Number of Glue DPUs (workers)"
  type        = number
  default     = 2
}
