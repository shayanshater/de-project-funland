# resource "local_file" "ingestion" {
#     content = <<EOF
#     ingestion_bucket="${aws_s3_bucket.ingestion_bucket.bucket}"
#     processed_bucket="${aws_s3_bucket.processed_bucket.bucket}"
#     EOF
#     filename = "${path.module}/../terraform.env" 
# }

#TODO: How to access bucket names from the python files? 

