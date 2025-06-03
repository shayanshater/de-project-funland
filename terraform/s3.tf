resource "aws_s3_bucket" "ingestion_bucket" {
  #TODO: Provision an S3 bucket for the data. 
  #TODO: Your bucket will need a unique, but identifiable name. Hint: Use the vars. 
  #TODO: Make sure to add an appropriate tag to this resource

  bucket_prefix = var.ingestion_bucket_prefix
  
  tags={
    Name="Ingestion Data Bucket"
    Environment="dev"
  }
}



#Creation of the processed bucket 

resource "aws_s3_bucket" "processed_bucket" {

  bucket_prefix = var.processed_bucket_prefix
  
  tags={
    Name="Processed Data Bucket"
    Environment="dev"
  }
}
