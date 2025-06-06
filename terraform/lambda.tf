# we are zipping the python code for the Lambda function
data "archive_file" "lambda" {
  type             = "zip"
  source_dir       = "${path.module}/../src/lambda_handler"
  output_path      = "${path.module}/../deployment/handlers/lambda_handlers.zip"
}
## to install the packages run pip install -r requirements.txt -t dependencies/packages

# ziping the packages to lambda layer
data "archive_file" "lambda_layer" {
  type             = "zip"
  source_dir       = "${path.module}/../dependencies/packages"
  output_path      = "${path.module}/../dependencies/layers/lambda_handlers_layer.zip"
}




resource "aws_s3_object" "lambda_zip_file"{
  bucket = aws_s3_bucket.layer_bucket.bucket
  key    = "extract_layer"
  source = data.archive_file.lambda.output_path
}

# creating the lambda layer
resource "aws_lambda_layer_version" "lambda_layer" {
  # provisioner "local-exec" {
  # command = "pip install pandas -t dependencies/packages/pandas/"
  #"pip install pg8000 -t dependencies/packages/pg8000/"
  #"pip install awswrangler -t dependencies/packages/awswrangler/"
  # }
  layer_name          = "etl_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket = aws_s3_object.lambda_zip_file.bucket
  s3_key = aws_s3_object.lambda_zip_file.key
}


#we are creating the Lambda
resource "aws_lambda_function" "extract_lambda_handler" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/../deployment/handlers/lambda_handlers.zip"
  function_name = var.lambda_extract
  role          = aws_iam_role.lambda_role.arn
  handler       = "extract.lambda_handler"  
  runtime       = var.python_runtime
  timeout       = 900
  memory_size   = 3000

  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers = [aws_lambda_layer_version.lambda_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:17"]
  environment {
    variables = {
      S3_INGESTION_BUCKET = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}

resource "aws_lambda_function" "transform_lambda_handler" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/../deployment/handlers/lambda_handlers.zip"
  function_name = var.lambda_transform
  role          = aws_iam_role.lambda_role.arn
  handler       = "transform.lambda_handler"  
  runtime       = var.python_runtime

  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers = [aws_lambda_layer_version.lambda_layer.arn]
  environment {
    variables = {
      S3_INGESTION_BUCKET = aws_s3_bucket.ingestion_bucket.bucket
      S3_PROCESSED_BUCKET = aws_s3_bucket.processed_bucket.bucket
    }
  }
}

resource "aws_lambda_function" "load_lambda_handler" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/../deployment/handlers/lambda_handlers.zip"
  function_name = var.lambda_load
  role          = aws_iam_role.lambda_role.arn
  handler       = "load.lambda_handler"  
  runtime       = var.python_runtime

  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers = [aws_lambda_layer_version.lambda_layer.arn]

  environment {
    variables = {
      S3_PROCESSED_BUCKET = aws_s3_bucket.processed_bucket.bucket
    }
  }
}