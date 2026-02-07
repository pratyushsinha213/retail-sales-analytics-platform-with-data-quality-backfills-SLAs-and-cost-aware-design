# IAM role: pratyush_<resource_name>_<region>_<type>
resource "aws_iam_role" "pratyush_glue_job_region_role" {
  name = "pratyush_glue_job_${local.pratyush_region_normalized}_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "pratyush_glue_job_region_service" {
  role       = aws_iam_role.pratyush_glue_job_region_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "pratyush_glue_s3_region_policy" {
  name = "pratyush_glue_s3_${local.pratyush_region_normalized}_policy"
  role = aws_iam_role.pratyush_glue_job_region_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.pratyush_datalake_region_bucket.arn,
          "${aws_s3_bucket.pratyush_datalake_region_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "pratyush_glue_catalog_region_policy" {
  name = "pratyush_glue_catalog_${local.pratyush_region_normalized}_policy"
  role = aws_iam_role.pratyush_glue_job_region_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:BatchCreatePartition"
        ]
        Resource = ["*"]
      }
    ]
  })
}
