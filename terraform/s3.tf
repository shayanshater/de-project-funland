# Create ingestion bucket
resource "aws_s3_bucket" "ingestion_bucket" {
  # Bucket need a unique, but identifiable name.
  bucket_prefix = var.ingestion_bucket_prefix
  
  tags={
    Name="Ingestion Data Bucket"
    Environment="dev"
  }
}

# Creation of the processed bucket 
resource "aws_s3_bucket" "processed_bucket" {
  bucket_prefix = var.processed_bucket_prefix
  
  tags={
    Name="Processed Data Bucket"
    Environment="dev"
  }
}
