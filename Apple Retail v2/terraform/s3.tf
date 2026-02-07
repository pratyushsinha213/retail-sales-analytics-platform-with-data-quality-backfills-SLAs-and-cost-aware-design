# S3 bucket: pratyush_<resource_name>_<region>_<type>
# Bucket name uses hyphens (AWS requirement); Terraform resource label uses underscores.
resource "aws_s3_bucket" "pratyush_datalake_region_bucket" {
  bucket = var.pratyush_s3_bucket_name

  tags = {
    Name        = var.pratyush_s3_bucket_name
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "pratyush_datalake_region_versioning" {
  bucket = aws_s3_bucket.pratyush_datalake_region_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pratyush_datalake_region_encryption" {
  bucket = aws_s3_bucket.pratyush_datalake_region_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pratyush_datalake_region_public_block" {
  bucket = aws_s3_bucket.pratyush_datalake_region_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
