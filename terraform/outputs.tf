output "raw_bucket_arn" {
  description = "ARN of the S3 Raw Data Lake Bucket"
  value       = aws_s3_bucket.raw.arn
}

output "processed_bucket_arn" {
  description = "ARN of the S3 Processed Data Lake Bucket"
  value       = aws_s3_bucket.processed.arn
}

output "glue_job_name" {
  description = "Name of the provisioned AWS Glue ETL Job"
  value       = aws_glue_job.football_etl.name
}

output "glue_catalog_database" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.football_analytics.name
}

output "athena_workgroup_name" {
  description = "Name of the Athena Workgroup"
  value       = aws_athena_workgroup.analytics.name
}
