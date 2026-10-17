variable "project_name" {
  description = "Name identifier for the project resources"
  type        = string
  default     = "football-analytics"
}

variable "environment" {
  description = "Target deployment environment (dev, staging, production)"
  type        = string
  default     = "production"
}

variable "aws_region" {
  description = "AWS deployment region"
  type        = string
  default     = "us-east-1"
}

variable "raw_bucket_name" {
  description = "Globally unique name for the S3 Raw Data Lake Zone"
  type        = string
  default     = "football-analytics-lake-raw"
}

variable "staging_bucket_name" {
  description = "Globally unique name for the S3 Staging Data Lake Zone"
  type        = string
  default     = "football-analytics-lake-staging"
}

variable "processed_bucket_name" {
  description = "Globally unique name for the S3 Processed Data Lake Zone"
  type        = string
  default     = "football-analytics-lake-processed"
}

variable "glue_catalog_database_name" {
  description = "Name of the AWS Glue Data Catalog Database"
  type        = string
  default     = "football_analytics_db"
}

variable "glue_job_name" {
  description = "Name of the AWS Glue PySpark ETL Job"
  type        = string
  default     = "glue-transform-football-match-stats"
}
