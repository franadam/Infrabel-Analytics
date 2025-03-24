variable "region" {
  description = "Region"
  default     = "eu-west-3"
}

variable "vpc_cidr_block" {
  description = "My VPC CIDR"
  default     = "10.0.0.0/16"
}

variable "vpc_tag_name" {
  description = "My VPC Name"
  default     = "infrabel-vpc"
}

variable "s3_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "infrabel-bucket"
}

variable "s3_bucket_tag_name" {
  description = "My bucket tag Name"
  default     = "infrabel-bucket"
}

variable "s3_bucket_tag_environment" {
  description = "My bucket Environment"
  default     = "Dev"
}

variable "athena_staging_database_name" {
  description = "My Athena staging database tag Name"
  default     = "infrabel_staging_db"
}

variable "athena_dw_database_name" {
  description = "My Athena dw database tag Name"
  default     = "infrabel_dw_db"
}