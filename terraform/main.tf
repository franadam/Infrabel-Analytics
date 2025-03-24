terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# add region
provider "aws" {
  region = var.region
}

# Create a s3 bucket
resource "aws_s3_bucket" "bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true

  tags = {
    Name = var.s3_bucket_tag_name
    Environment = var.s3_bucket_tag_environment 
  }
}

# Create an athena_database
resource "aws_athena_database" "staging" {
  name   = var.athena_staging_database_name
  bucket = var.s3_bucket_name
}

resource "aws_athena_database" "dw" {
  name   = var.athena_dw_database_name
  bucket = var.s3_bucket_name
}