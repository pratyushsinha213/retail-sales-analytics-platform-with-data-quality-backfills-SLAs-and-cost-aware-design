terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }

  # Optional: remote backend (uncomment and set for team use)
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "apple-retail/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  # }
}

locals {
  # Region with hyphens replaced by underscores for resource naming (e.g. us_east_1)
  pratyush_region_normalized = replace(var.aws_region, "-", "_")
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
