
variable "ingestion_bucket_prefix" {
  type    = string
  default = "funland-ingestion-bucket-"
}


variable "processed_bucket_prefix" {
  type    = string
  default = "funland-processed-bucket-"
}

variable "lambda_extract" {
  type    = string
  default = "extract-lambda"
}

variable "lambda_transform" {
  type    = string
  default = "transform-lambda"
}

variable "lambda_load" {
  type    = string
  default = "load-lambda"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

variable "step_function" {
  type = string
  default = "funland-etl"
}

variable "scheduler" {
  type = string
  default = "funland-etl-schedule"  
}

variable "notification_email" { # TODO: figure our how to use .tfvars
  description = "Email address to receive Lambda failure notifications"
  type        = string
}

variable "aws_region" {
  type    = string
  default = "eu-west-2"
}