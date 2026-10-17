terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "DataEngineering"
    }
  }
}

# ==============================================================================
# S3 Data Lake Buckets (Bronze / Silver / Gold Architecture)
# ==============================================================================

# 1. Raw Zone (Bronze Lake)
resource "aws_s3_bucket" "raw" {
  bucket        = var.raw_bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-raw-to-glacier"
    status = "Enabled"

    filter {
      prefix = "matches/raw/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# 2. Staging Zone
resource "aws_s3_bucket" "staging" {
  bucket        = var.staging_bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "staging" {
  bucket = aws_s3_bucket.staging.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. Processed Zone (Silver / Gold Lake - Parquet)
resource "aws_s3_bucket" "processed" {
  bucket        = var.processed_bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket = aws_s3_bucket.processed.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ==============================================================================
# IAM Roles & Policies for AWS Glue
# ==============================================================================

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-service-role-${var.environment}"

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
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_access" {
  name        = "${var.project_name}-glue-s3-policy-${var.environment}"
  description = "Allows Glue job full access to project data lake S3 buckets"

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
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.staging.arn,
          "${aws_s3_bucket.staging.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

# ==============================================================================
# AWS Glue Catalog & PySpark ETL Job
# ==============================================================================

resource "aws_glue_catalog_database" "football_analytics" {
  name        = var.glue_catalog_database_name
  description = "Catalog database for European Football Analytics Lakehouse"
}

resource "aws_s3_object" "glue_pyspark_script" {
  bucket = aws_s3_bucket.processed.id
  key    = "scripts/transform_match_stats.py"
  source = "${path.module}/../scripts/transformation/transform_match_stats.py"
  etag   = filemd5("${path.module}/../scripts/transformation/transform_match_stats.py")
}

resource "aws_glue_job" "football_etl" {
  name         = var.glue_job_name
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 4
  timeout      = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.processed.id}/${aws_s3_object.glue_pyspark_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.project_name}"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--S3_RAW_PATH"                      = "s3://${aws_s3_bucket.raw.id}/matches/raw/"
    "--S3_PROCESSED_PATH"                = "s3://${aws_s3_bucket.processed.id}/matches/processed/match_stats/"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy_attachment.glue_s3_attach,
    aws_s3_object.glue_pyspark_script
  ]
}

# ==============================================================================
# Amazon Athena Analytics Workgroup
# ==============================================================================

resource "aws_athena_workgroup" "analytics" {
  name        = "${var.project_name}-workgroup"
  description = "Dedicated Athena workgroup for analytical football KPI queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.processed.id}/athena-query-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
