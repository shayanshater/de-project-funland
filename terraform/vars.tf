
variable "ingestion_bucket_prefix" {
  type    = string
  default = "funland-ingestion-bucket-"
}


variable "processed_bucket_prefix" {
  type    = string
  default = "funland-processed-bucket-"
}

variable "lambda_ingestion" {
  type    = string
  default = "ingestion"
}

variable "python_runtime" {
  type    = string
  default = "python3.13"
}

