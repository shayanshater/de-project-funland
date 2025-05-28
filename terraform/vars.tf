
variable "ingestion_bucket" {
  type    = string
  default = "funland-ingestion-bucket-11"
}

variable "lambda_ingestion" {
  type    = string
  default = "ingestion"
}

variable "python_runtime" {
  type    = string
  default = "python3.13"
}
