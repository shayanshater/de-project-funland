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

# creating the lambda layer
resource "aws_lambda_layer_version" "lambda_layer" {
  # provisioner "local-exec" {
  #   command = "pip install -r requirements.txt -t dependencies/packages/"
  #   }

  layer_name          = "etl_layer"
  filename = data.archive_file.lambda_layer.output_path
  source_code_hash    = data.archive_file.lambda_layer.output_base64sha256
  compatible_runtimes = [var.python_runtime]
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

  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers = [aws_lambda_layer_version.lambda_layer.arn]
  environment {
    variables = {
      s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
      db_password = "the_password"
    #add the .env? 
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
      s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
    #add the .env? 
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
      s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
    #add the .env? 
    }
  }
}