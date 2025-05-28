resource "aws_s3_bucket" "ingestion_bucket" {
  #TODO: Provision an S3 bucket for the data. 
  #TODO: Your bucket will need a unique, but identifiable name. Hint: Use the vars. 
  #TODO: Make sure to add an appropriate tag to this resource

  bucket = var.ingestion_bucket
  
  tags={
    Name="Ingestion Data BUcket"
    Environment="dev"
  }
}
